/**
 * Copyright (C) 2014-2015 Really Inc. <http://really.io>
 */

package io.really.gorilla

import akka.util.Timeout
import io.really.protocol.SubscriptionFailure
import scala.collection.mutable.Map
import _root_.io.really.model.FieldKey
import akka.actor._
import _root_.io.really.{ R, ReallyGlobals }
import _root_.io.really.WrappedSubscriptionRequest.{ WrappedSubscribe, WrappedUnsubscribe }
import akka.pattern.{ AskTimeoutException, ask }

import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.control.NonFatal

class SubscriptionManager(globals: ReallyGlobals) extends Actor with ActorLogging {

  type SubscriberIdentifier = ActorPath

  import SubscriptionManager._

  var rSubscriptions: Map[SubscriberIdentifier, InternalSubscription] = Map.empty
  var roomSubscriptions: Map[SubscriberIdentifier, InternalSubscription] = Map.empty

  def failedToRegisterNewSubscription(originalSender: ActorRef, r: R, newSubscriber: ActorRef, reason: String) = {
    newSubscriber ! SubscriptionFailure(r, 500, reason)
    sender() ! SubscriptionFailure(r, 500, reason)
    log.error(reason)
  }

  def receive = {
    case request: WrappedSubscribe =>
      context.actorOf(Props(new SubscribeAggregator(self, globals))) forward request
    case request: WrappedUnsubscribe =>
      ???
    case SubscribeOnR(subData) =>
      rSubscriptions.get(subData.pushChannel.path).map {
        rSub =>
          rSub.objectSubscriber ! UpdateSubscriptionFields(subData.fields.getOrElse(Set.empty))
        //TODO Ack the delegate
      }.getOrElse {
        implicit val timeout = Timeout(globals.config.GorillaConfig.waitForGorillaCenter)
        val originalSender = sender()
        val result = globals.gorillaEventCenter ? NewSubscription(subData)
        result.onSuccess {
          case ObjectSubscribed(objectSubscriber) =>
            rSubscriptions += subData.pushChannel.path -> InternalSubscription(objectSubscriber, subData.r)
            context.watch(objectSubscriber) //TODO handle death
            context.watch(subData.pushChannel) //TODO handle death
            originalSender ! SubscriptionDone
          case _ =>
            val reason = s"Gorilla Center replied with unexpected response to new subscription request: $subData"
            failedToRegisterNewSubscription(originalSender, subData.r, subData.pushChannel, reason)
        }
        result.onFailure {
          case e: AskTimeoutException =>
            val reason = s"SubscriptionManager timed out waiting for the Gorilla center response for" +
              s" subscription $subData"
            failedToRegisterNewSubscription(originalSender, subData.r, subData.pushChannel, reason)
          case NonFatal(e) =>
            val reason = s"Unexpected error while asking the Gorilla Center to establish a new subscription: $subData"
            failedToRegisterNewSubscription(originalSender, subData.r, subData.pushChannel, reason)
        }
      }
    case SubscribeOnRoom(subData) => ??? //TODO Handle Room subscriptions
    case UnsubscribeFromR(subData) => //TODO Ack the delegate
      rSubscriptions.get(subData.pushChannel.path).map {
        rSub =>
          rSub.objectSubscriber ! Unsubscribe
          rSubscriptions -= subData.pushChannel.path
      }
    case UnsubscribeFromRoom(subData) =>
      roomSubscriptions.get(subData.pushChannel.path).map {
        roomSub =>
          roomSub.objectSubscriber ! Unsubscribe
          roomSubscriptions -= subData.pushChannel.path
      }
    case Terminated(actor) =>
      //TODO Handle death of subscribers
      log.info("Actor Terminated" + actor)
  }
}

object SubscriptionManager {

  case class InternalSubscription(objectSubscriber: ActorRef, r: R)

  case class SubscribeOnR(rSubscription: RSubscription)

  case class SubscribeOnRoom(rSubscription: RoomSubscription)

  case class UnsubscribeFromR(roomSubscription: RSubscription)

  case class UnsubscribeFromRoom(roomSubscription: RoomSubscription)

  case class UpdateSubscriptionFields(fields: Set[FieldKey])

  case object Unsubscribe

  case class ObjectSubscribed(objectSubscriber: ActorRef)

  case object SubscriptionDone

}
