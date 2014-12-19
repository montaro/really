/*
 * Copyright (C) 2014-2015 Really Inc. <http://really.io>
 */

package io.really.gorilla

import akka.actor._
import akka.util.Timeout
import io.really.gorilla.GorillaEventCenter.ReplayerSubscribed
import io.really.gorilla.SubscriptionManager.{ UpdateSubscriptionFields, Unsubscribe }
import io.really.ReallyGlobals
import io.really.model.FieldKey
import io.really.model.persistent.ModelRegistry.CollectionActorMessage.GetModel
import io.really.model.persistent.ModelRegistry.ModelResult
import io.really.model.persistent.ModelRegistry.ModelResult.ModelFetchError
import io.really.protocol.FieldUpdatedOp
import io.really.protocol.ProtocolFormats.PushMessageWrites.{ Created, Updated, Deleted }
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.control.NonFatal
import akka.pattern.{ AskTimeoutException, ask, pipe }
import io.really.model.Model
import io.really.protocol.SubscriptionFailure
import io.really.protocol.SubscriptionFailure.SubscriptionFailureWrites

class ObjectSubscriber(rSubscription: RSubscription, globals: ReallyGlobals) extends Actor with ActorLogging
    with Stash {

  val r = rSubscription.r
  val logTag = s"ObjectSubscriber ${rSubscription.pushChannel.path}$$$r"

  var fields: Set[FieldKey] = rSubscription.fields match {
    case Some(fs) => Set(fs.toSeq: _*)
    case None => Set.empty
  }

  val shotgun = context.system.scheduler.scheduleOnce(globals.config.GorillaConfig.waitForReplayer, self, PoisonPill)

  def subscriptionFailed(errorCode: Int, reason: String) = {
    log.error(s"$logTag is going to die since the subscription failed because of: $reason\n error code: $errorCode")
    rSubscription.requestDelegate ! SubscriptionFailureWrites.writes(SubscriptionFailure(r, errorCode, reason))
    context.stop(self)
  }

  def commonHandler: Receive = {
    case Unsubscribe =>
      log.debug(s"$logTag is going to die since it got an unsubscribe request")
      rSubscription.requestDelegate ! Unsubscribe
      context.stop(self)
    case SubscriptionFailure(r, errorCode, reason) =>
      subscriptionFailed(errorCode, "Internal Server Error")
    case Terminated(actor) =>
      subscriptionFailed(505, "Associated replayer stopped")
  }

  def receive: Receive = commonHandler orElse starterReceive

  def starterReceive: Receive = {
    //TODO Set timeout for waiting the Replayer starting
    case ReplayerSubscribed(replayer) =>
      context.watch(replayer)
      implicit val timeout = Timeout(globals.config.GorillaConfig.waitForModel)
      val f = (globals.modelRegistry ? GetModel(rSubscription.r, self)).mapTo[ModelResult]
      f.recoverWith {
        case e: AskTimeoutException =>
          log.debug(s"$logTag timed out waiting for the model object")
          Future successful ModelResult.ModelFetchError(r, s"Request to fetch the model timed out for R: $r")
        case NonFatal(e) =>
          log.error(e, s"$logTag got an unexpected error while getting the model instance")
          Future successful ModelResult.ModelFetchError(r, s"Request to fetch the model failed for R: $r with error: $e")
      } pipeTo self
      shotgun.cancel()
      unstashAll()
      context.become(commonHandler orElse waitingModel)
    case _ =>
      stash()
  }

  def waitingModel: Receive = {
    case ModelResult.ModelNotFound =>
      subscriptionFailed(503, s"$logTag couldn't find the model for r: $r")
    case evt @ ModelResult.ModelObject(m, _) =>
      log.debug(s"$logTag found the model for r: $r")
      if (fields.isEmpty)
        fields = evt.model.fields.keySet
      unstashAll()
      context.become(withModel(m) orElse commonHandler)
    case ModelFetchError(r, reason) =>
      subscriptionFailed(504, s"$logTag couldn't fetch the model for r: $r because of: $reason")
    case _ =>
      stash()
  }

  def withModel(model: Model): Receive = {
    case entry: GorillaLogCreatedEntry =>
      if (entry.modelVersion == model.collectionMeta.version) {
        model.executeOnGet(rSubscription.ctx, globals, entry.obj) match {
          case Right(plan) =>
            val interestFields = fields -- plan.hidden
            var newObj = entry.obj
            interestFields.foreach(field => newObj = newObj - field)
            Created.toJson(entry.r, newObj)
          case Left(terminated) =>
        }
      } else {
        subscriptionFailed(502, "Model Version inconsistency")
      }
    case entry: GorillaLogUpdatedEntry =>
      if (entry.modelVersion == model.collectionMeta.version) {
        model.executeOnGet(rSubscription.ctx, globals, entry.obj) match {
          case Right(plan) =>
            val interestFields = fields -- plan.hidden
            val updatedFields = entry.ops.filter(op => interestFields.contains(op.key)).map {
              op =>
                FieldUpdatedOp(op.key, op.op, Some(op.value), entry.userInfo.userR)
            }
            rSubscription.pushChannel ! Updated.toJson(r, entry.rev, updatedFields)
          case Left(terminated) =>
        }
      } else {
        subscriptionFailed(502, "Model Version inconsistency")
      }
    case entry: GorillaLogDeletedEntry =>
      rSubscription.pushChannel ! Deleted.toJson(entry.userInfo.userR, r)
      context.stop(self)
    case UpdateSubscriptionFields(newFields) =>
      if (newFields.isEmpty) {
        fields = model.fields.keySet
      } else {
        fields = fields union Set(newFields.toSeq: _*)
      }
    case ModelUpdatedEvent(_, newModel) =>
      log.debug(s"$logTag received a ModelUpdated message for: $r")
      context.become(withModel(newModel) orElse commonHandler)
    case ModelDeletedEvent(deletedR) =>
      if (deletedR == r) {
        rSubscription.requestDelegate ! SubscriptionFailure(r, 501, s"received a DeletedModel message for: $r")
        context.stop(self)
      }
  }

}