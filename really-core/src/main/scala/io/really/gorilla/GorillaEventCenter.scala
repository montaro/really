/**
 * Copyright (C) 2014-2015 Really Inc. <http://really.io>
 */
package io.really.gorilla

import akka.actor._
import akka.contrib.pattern.DistributedPubSubMediator.Subscribe
import akka.contrib.pattern.ShardRegion
import io.really.gorilla.SubscriptionManager.ObjectSubscribed
import io.really.model.{ Model, Helpers }
import io.really._

import scala.slick.driver.H2Driver.simple._
import scala.slick.jdbc.meta.MTable

/**
 * The gorilla event centre is an actor that receives events from the Collection View Materializer
 * and store it in a persistent ordered store (MongoDB per collection (R) capped collection)
 * and return on success a confirmation EventStored to the view materializer to proceed with he next message.
 * It ensures that we are not storing the same revision twice for the same object (by ignoring the event)
 * while confirming that it's stored (faking store).
 * publishes the event on the Gorilla PubSub asynchronously for real-time event distribution
 * @param globals
 */
class GorillaEventCenter(globals: ReallyGlobals)(implicit session: Session) extends Actor with ActorLogging {
  import GorillaEventCenter._

  val bucketID: BucketID = self.path.name

  val r: R = Helpers.getRFromBucketID(bucketID)

  private[this] val config = globals.config.EventLogStorage

  private[this] var maxMarkers: Map[R, Revision] = Map.empty

  def receive: Receive = handleEvent orElse handleSubscriptions

  def handleEvent: Receive = {
    case msg: PersistentEvent =>
      sender ! persistEvent(msg)
    //todo pass it to the pubsub

    case evt: StreamingEvent =>
    //todo pass it silently to the pubsub

    case ModelUpdatedEvent(_, model) =>
      removeOldModelEvents(model)
    //todo notify the replayers with model updates
  }

  def handleSubscriptions: Receive = {
    case NewSubscription(rSub) =>
      val objectSubscriber = context.actorOf(globals.objectSubscriberProps(rSub))
      maxMarkers.get(r) match {
        case Some(rev) =>
          val replayer = context.actorOf(Props(new Replayer(globals, objectSubscriber, rSub, Some(rev))))
          globals.mediator ! Subscribe(rSub.r.toString, replayer)
          objectSubscriber ! ReplayerSubscribed(replayer)
        case None =>
          val replayer = context.actorOf(Props(new Replayer(globals, objectSubscriber, rSub, None)))
          globals.mediator ! Subscribe(rSub.r.toString, replayer)
          objectSubscriber ! ReplayerSubscribed(replayer)
      }
      sender() ! ObjectSubscribed(objectSubscriber)
    case Terminated(actor) =>
      log.info("Actor Terminated" + actor)

  }

  private def persistEvent(persistentEvent: PersistentEvent): GorillaLogResponse =
    persistentEvent match {
      case PersistentCreatedEvent(event) =>
        events += EventLog("created", event.r, 1l, event.modelVersion, event.obj,
          event.context.auth, None)
        EventStored
      case PersistentUpdatedEvent(event, obj) =>
        events += EventLog("updated", event.r, event.rev, event.modelVersion, obj,
          event.context.auth, Some(event.ops))
        EventStored
      case _ => GorillaLogError.UnsupportedEvent
    }

  private def removeOldModelEvents(model: Model) =
    events.filter(_.ModelVersion < model.collectionMeta.version).delete
}

object GorillaEventCenter {
  // the query interface for the log table
  val events: TableQuery[EventLogs] = TableQuery[EventLogs]

  /**
   * Create the events table
   * @param session
   * @return
   */
  def initializeDB()(implicit session: Session) =
    if (MTable.getTables(EventLogs.tableName).list.isEmpty) {
      events.ddl.create
    }

  case class ReplayerSubscribed(replayer: ActorRef)

}

class GorillaEventCenterSharding(config: ReallyConfig) {

  implicit val implicitConfig = config

  val maxShards = config.Sharding.maxShards

  /**
   * ID Extractor for Akka Sharding extension
   * ID is the BucketId
   */
  val idExtractor: ShardRegion.IdExtractor = {
    case req: RoutableToGorillaCenter => Helpers.getBucketIDFromR(req.r) -> req
    case modelEvent: ModelEvent => modelEvent.bucketID -> modelEvent
  }

  /**
   * Shard Resolver for Akka Sharding extension
   */
  val shardResolver: ShardRegion.ShardResolver = {
    case req: RoutableToGorillaCenter => (Helpers.getBucketIDFromR(req.r).hashCode % maxShards).toString
    case modelEvent: ModelEvent => (modelEvent.bucketID.hashCode % maxShards).toString
  }
}
