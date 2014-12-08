/*
 * Copyright (C) 2014-2015 Really Inc. <http://really.io>
 */

package io.really.gorilla

import akka.actor.{ ActorLogging, ActorRef, Actor }
import akka.contrib.pattern.Aggregator
import io.really.R
import io.really.RequestContext
import io.really.WrappedSubscriptionRequest.WrappedSubscribe
import io.really.gorilla.SubscriptionManager.{ SubscriptionDone, SubscribeOnR }
import io.really.protocol.{ SubscriptionFailure, SubscriptionBody }
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._

case object TimedOut

class SubscribeAggregator(subscriptionManager: ActorRef) extends Actor with Aggregator with ActorLogging {

  import context._

  expectOnce {
    case WrappedSubscribe(subscribeRequest, pushChannel) => new SubscribeAggregatorImpl(
      subscribeRequest.ctx, sender(), subscribeRequest.body, pushChannel
    )
    case msg =>
      log.error(s"Subscribe Aggregator got an unexpected response: $msg and going to die")
      context.stop(self)
  }

  class SubscribeAggregatorImpl(ctx: RequestContext, requestDelegate: ActorRef, body: SubscriptionBody,
      pushChannel: ActorRef) {

    val results = ArrayBuffer.empty[R]

    if (body.subscriptions.size > 0) {
      body.subscriptions.foreach {
        op =>
          subscribeOnR(RSubscription(ctx, op.r, Some(op.fields), op.rev, requestDelegate, pushChannel))
      }
    } else {
      collectSubscriptions()
    }

    context.system.scheduler.scheduleOnce(3.second, self, TimedOut) //TODO Needs to be a configuration
    expect {
      case TimedOut => collectSubscriptions(force = true)
    }

    def subscribeOnR(rSub: RSubscription) = {
      subscriptionManager ! SubscribeOnR(rSub)
      expectOnce {
        case SubscriptionDone =>
          results += rSub.r
          collectSubscriptions()
        case sf: SubscriptionFailure =>
      }
    }

    def collectSubscriptions(force: Boolean = false) {
      println("\n----------Collecting")
      if (results.size == body.subscriptions.size || force) {
        requestDelegate ! SubscribeAggregator.Subscribed(results.toSet)
        context.stop(self)
      }
    }
  }

}

object SubscribeAggregator {

  case class Subscribed(rs: Set[R])

}
