/**
 * Copyright (C) 2014-2015 Really Inc. <http://really.io>
 */

package io.really.gorilla

import akka.util.Timeout
import io.really.protocol.SubscriptionFailure
import scala.collection.mutable.Map
import _root_.io.really.model.FieldKey
import akka.actor._
import _root_.io.really.{ R, CID, ReallyGlobals }
import akka.pattern.{ AskTimeoutException, ask }

import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.control.NonFatal

class SubscriptionManager(globals: ReallyGlobals) extends Actor with ActorLogging {

  import SubscriptionManager._

  var rSubscriptions: Map[CID, InternalSubscription] = Map.empty
  var roomSubscriptions: Map[CID, InternalSubscription] = Map.empty

  def receive = {
    case SubscribeOnR(subData) =>
      rSubscriptions.get(subData.cid).map {
        rSub =>
          rSub.subscriptionActor ! UpdateSubscriptionFields(subData.fields.getOrElse(Set.empty))
      }.getOrElse {
        val newSubscriber = context.actorOf(globals.objectSubscriberProps(subData), subData.r.actorFriendlyStr + "$"
          + subData.cid)
        implicit val timeout = Timeout(globals.config.GorillaConfig.waitForGorillaCenter)
        val result = globals.gorillaEventCenter ? NewSubscription(subData, newSubscriber)
        result.onSuccess {
          case Subscribed =>
            rSubscriptions += subData.cid -> InternalSubscription(newSubscriber, subData.r)
            context.watch(newSubscriber)
            newSubscriber ! Subscribed
          case _ =>
            val reason = s"Gorilla Center replied with unexpected response to new subscription request: $subData"
            log.error(reason)
            newSubscriber ! SubscriptionFailure(subData.r, 500, reason)
        }
        result.onFailure {
          case e: AskTimeoutException =>
            val reason = s"SubscriptionManager timed out waiting for the Gorilla center response for" +
              s" subscription $subData"
            log.error(reason)
            newSubscriber ! SubscriptionFailure(subData.r, 500, reason)
          case NonFatal(e) =>
            val reason = s"Unexpected error while asking the Gorilla Center to establish a new subscription: $subData"
            log.error(reason)
            newSubscriber ! SubscriptionFailure(subData.r, 500, reason)
        }
      }
    case SubscribeOnRoom(subData) => ??? //TODO Handle Room subscriptions
    case UnsubscribeFromR(subData) =>
      rSubscriptions.get(subData.cid).map {
        rSub =>
          rSub.subscriptionActor ! Unsubscribe
          rSubscriptions -= subData.cid
      }
    case UnsubscribeFromRoom(subData) =>
      roomSubscriptions.get(subData.cid).map {
        roomSub =>
          roomSub.subscriptionActor ! Unsubscribe
          roomSubscriptions -= subData.cid
      }
    case Terminated(actor) => ??? //TODO Handle death of subscribers
  }
}

object SubscriptionManager {

  case class InternalSubscription(subscriptionActor: ActorRef, r: R)

  case class SubscribeOnR(rSubscription: RSubscription)

  case class SubscribeOnRoom(rSubscription: RoomSubscription)

  case class UnsubscribeFromR(roomSubscription: RSubscription)

  case class UnsubscribeFromRoom(roomSubscription: RoomSubscription)

  case class UpdateSubscriptionFields(fields: Set[FieldKey])

  case object Unsubscribe

  case object Subscribed

}
