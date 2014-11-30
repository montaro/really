/*
 * Copyright (C) 2014-2015 Really Inc. <http://really.io>
 */

package io.really.gorilla

import akka.actor.{ Terminated, Props, ActorSystem }
import akka.testkit.{ TestProbe, TestActorRef }
import io.really._
import _root_.io.really.gorilla.SubscriptionManager.{ Subscribed, UpdateSubscriptionFields, Unsubscribe }
import _root_.io.really.protocol.ProtocolFormats.PushMessageWrites.Deleted

import scala.slick.driver.H2Driver.simple._

class ObjectSubscriberSpec extends BaseActorSpecWithMongoDB {

  val caller = TestProbe()
  val requestDelegate = TestProbe()
  val pushChannel = TestProbe()
  val deathPrope = TestProbe()
  val rev: Revision = 1L
  val cid: CID = "99"
  val r: R = R / 'users / 1
  val rSub = RSubscription(ctx, cid, r, None, rev, requestDelegate.ref, pushChannel.ref)
  val userInfo = UserInfo(AuthProvider.Anonymous, R("/_anonymous/1234567"), Application("reallyApp"))

  "Object Subscription" should "Initialized Successfully" in {
    val rSub1 = RSubscription(ctx, cid, r, Some(Set("name")), rev, requestDelegate.ref, pushChannel.ref)
    val objectSubscriberActor = TestActorRef[ObjectSubscriber](globals.objectSubscriberProps(rSub1))
    objectSubscriberActor.tell(Subscribed, caller.ref)
    objectSubscriberActor.underlyingActor.fields shouldEqual Set("name")
  }

  it should "Update Internal field List Successfully" in {
    val rSub1 = RSubscription(ctx, cid, r, Some(Set("name")), rev, requestDelegate.ref, pushChannel.ref)
    val objectSubscriberActor = TestActorRef[ObjectSubscriber](globals.objectSubscriberProps(rSub1))
    objectSubscriberActor.tell(Subscribed, caller.ref)
    caller.expectNoMsg()
    objectSubscriberActor.tell(UpdateSubscriptionFields(Set("age")), caller.ref)
    caller.expectNoMsg()
    objectSubscriberActor.underlyingActor.fields shouldEqual Set("name", "age")

  }

  it should "unsubscribe Successfully and self terminated" in {
    val rSub1 = RSubscription(ctx, cid, r, Some(Set("name")), rev, requestDelegate.ref, pushChannel.ref)
    val objectSubscriberActor = TestActorRef[ObjectSubscriber](globals.objectSubscriberProps(rSub1))
    objectSubscriberActor ! Subscribed
    objectSubscriberActor.underlyingActor.fields shouldEqual Set("name")
    objectSubscriberActor.tell(SubscriptionManager.Unsubscribe, caller.ref)
    requestDelegate.expectMsg(SubscriptionManager.Unsubscribe)
    deathPrope.watch(objectSubscriberActor)
    deathPrope.expectTerminated(objectSubscriberActor)
  }

  it should "pass updates to push channel actor" in {
    val rSub1 = RSubscription(ctx, cid, r, Some(Set("name")), rev, requestDelegate.ref, pushChannel.ref)
    val objectSubscriberActor = TestActorRef[ObjectSubscriber](globals.objectSubscriberProps(rSub1))
    objectSubscriberActor ! Subscribed
    //objectSubscriberActor ! GorillaLogCreatedEntry(r, userInfo)
    //requestDelegate.expectMsg(Deleted.toJson(userInfo, r))
    deathPrope.watch(objectSubscriberActor)
    deathPrope.expectTerminated(objectSubscriberActor)
  }
}