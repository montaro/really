/*
 * Copyright (C) 2014-2015 Really Inc. <http://really.io>
 */

package io.really.gorilla

import akka.actor._
import akka.contrib.pattern.DistributedPubSubMediator.Unsubscribe
import akka.util.Timeout
import io.really.gorilla.Replayer.SnapshotResult._
import io.really.gorilla.Replayer.{ EndOfEventsLog, SnapshotResult }
import io.really.model.CollectionActor.{ GetState, State }
import io.really.{ R, Revision, ReallyGlobals }
import akka.pattern.{ AskTimeoutException, ask, pipe }
import play.api.libs.json.JsObject
import io.really.CommandError.ObjectNotFound
import io.really.protocol.SubscriptionFailure

import scala.concurrent.Future
import scala.slick.lifted.TableQuery
import scala.slick.driver.H2Driver.simple._
import scala.util.control.NonFatal
import scala.concurrent.ExecutionContext.Implicits.global
import EventLogs._

class Replayer(globals: ReallyGlobals, objectSubscriber: ActorRef, rSubscription: RSubscription,
    maxMarker: Option[Revision])(implicit session: Session) extends Actor with ActorLogging with Stash {

  val replayerId = s"Replayer ${rSubscription.pushChannel.path}$$${rSubscription.r}"

  val min = rSubscription.rev
  private[gorilla] val r = rSubscription.r

  context.watch(objectSubscriber)
  //TODO Watch Gorilla to notify the ObjectSubscriber that the subscription failed in case of Gorilla's termination

  maxMarker.map {
    max: Revision =>
      if (min == max) {
        context.become(servePushUpdates)
      } else if (min > max && (min - max) <= globals.config.GorillaConfig.advancedRevisionLimit) {
        context.become(waitingCorrectRevision)
      } else if (min > max) {
        shutdownWithObjectSubscriptionAck("Subscription revision advances the current known revision with more than" +
          " the advancing limit")
      } else {
        getEvents(r, min)
      }
  }.getOrElse {
    getEvents(r, min)
  }

  def commonHandler: Receive = {
    case SubscriptionManager.Unsubscribe =>
      shutdown("Received unsubscribe request")
    case Terminated(`objectSubscriber`) =>
      shutdown("Object Subscription Actor was stopped")
    case msg =>
      unexpectedMessage(msg.toString)
  }

  def receive: Receive = starterReceiver orElse commonHandler

  def starterReceiver: Receive = {
    case _: GorillaLogEntry =>
      stash()
    case SnapshotObject(obj) =>
      //TODO State should have a revision field
      val stateRev = (obj \ "_rev").as[Long]
      if (min > stateRev && (min - stateRev) <= globals.config.GorillaConfig.advancedRevisionLimit) {
        context.become(waitingCorrectRevision)
      } else if (min > stateRev) {
        shutdownWithObjectSubscriptionAck("Subscription revision advances the current known revision with more than" +
          " the advancing limit")
      } else {
        objectSubscriber ! SnapshotObject(obj)
        context.become(servePushUpdates)
      }
    case SnapshotFetchError(r, _) =>
      val msg = "Failed to retrieve a snapshot for that object"
      shutdownWithObjectSubscriptionAck(msg)
  }

  def shutdown(reason: String) = {
    log.warning(s"$replayerId actor is going to die because of: $reason")
    globals.mediator ! Unsubscribe(r.toString, self)
    context.stop(self)
  }

  def shutdownWithObjectSubscriptionAck(reason: String) = {
    shutdown(reason)
    objectSubscriber ! SubscriptionFailure(r, 500, reason)
  }

  def unexpectedMessage(msg: String) = log.warning(s"$replayerId has received an unexpected message: $msg")

  def flushLogEvents(logs: List[EventLog]) = {
    logs.foreach {
      log =>
        self ! log
    }
    self ! EndOfEventsLog
    context.become(serveLogEvents)
  }

  def waitingCorrectRevision: Receive = _waitingCorrectRevision orElse commonHandler

  def _waitingCorrectRevision: Receive = {
    case entry: GorillaLogEntry if (entry.rev == min) =>
      context.become(servePushUpdates)
    case entry: GorillaLogEntry if (entry.rev > min + 1) =>
      shutdownWithObjectSubscriptionAck("Inconsistent push updates revision")
    case entry: GorillaLogEntry =>
    case entry: PersistentEvent if (entry.event.rev == min) =>
      context.become(servePushUpdates)
    case entry: PersistentEvent if (entry.event.rev > min + 1) =>
      shutdownWithObjectSubscriptionAck("Inconsistent push updates revision")
    case entry: PersistentEvent =>
  }

  def serveLogEvents: Receive = _serveLogEvents orElse commonHandler

  def _serveLogEvents: Receive = {
    case EventLog(event, r, rev, modelVersion, obj, userInfo, ops) =>
      event match {
        case "created" =>
          objectSubscriber ! GorillaLogCreatedEntry(r, obj, rev, modelVersion, userInfo)
        case "updated" =>
          objectSubscriber ! GorillaLogUpdatedEntry(r, obj, rev, modelVersion, userInfo,
            ops.getOrElse(List.empty))
        case "deleted" =>
          objectSubscriber ! GorillaLogDeletedEntry(r, rev, modelVersion, userInfo)
        case _ =>
      }
    case EndOfEventsLog =>
      unstashAll()
      context.become(servePushUpdates)
  }

  def servePushUpdates: Receive = _servePushUpdates orElse commonHandler

  def _servePushUpdates: Receive = {
    case PersistentCreatedEvent(event) =>
      objectSubscriber ! GorillaLogCreatedEntry(event.r, event.obj, 1L, event.modelVersion, event.context.auth)
    case PersistentUpdatedEvent(event, obj) =>
      objectSubscriber ! GorillaLogUpdatedEntry(event.r, obj, event.rev, event.modelVersion, event.context.auth,
        event.ops)
    case PersistentDeletedEvent(event) =>
      objectSubscriber ! GorillaLogDeletedEntry(event.r, event.rev, event.modelVersion, event.context.auth)
    case SubscriptionManager.Unsubscribe =>
      shutdownWithObjectSubscriptionAck("Received unsubscribe request")
  }

  def getEvents(r: R, min: Revision) = {
    val events: TableQuery[EventLogs] = TableQuery[EventLogs]
    val logEvents: List[EventLog] = events.filter(log => log.r === r && log.rev >= min).sortBy(_.rev.asc).list
    if (logEvents.isEmpty) {
      getSnapshot(r)
    } else {
      val minLogEvent = logEvents.head
      if (minLogEvent.rev > min + 1) {
        getSnapshot(r)
      } else {
        flushLogEvents(logEvents)
      }
    }
  }

  def getSnapshot(r: R) = {
    implicit val timeout = Timeout(globals.config.GorillaConfig.waitForSnapshot)
    val state = globals.collectionActor ? GetState(r)
    val result: Future[SnapshotResult] = state.map {
      case s: State =>
        SnapshotObject(s.obj)
      case ObjectNotFound(_) =>
        SnapshotFetchError(r, "Object Not Found")
    }
    state.recoverWith {
      case e: AskTimeoutException =>
        log.debug(s"$replayerId timed out waiting for the snapshot object")
        Future successful SnapshotResult.SnapshotFetchError(r, s" $replayerId Request to fetch the snapshot timed out")
      case NonFatal(e) =>
        log.error(e, s"Unexpected error while getting the snapshot instance for $replayerId")
        Future successful SnapshotResult.SnapshotFetchError(r, s"Request to fetch the snapshot failed for" +
          s" $replayerId with error: $e")
    }
    result pipeTo self
  }
}

object Replayer {

  sealed trait SnapshotResult

  object SnapshotResult {

    case class SnapshotObject(obj: JsObject) extends SnapshotResult

    case class SnapshotFetchError(r: R, reason: String) extends SnapshotResult

  }

  case object EndOfEventsLog

}
