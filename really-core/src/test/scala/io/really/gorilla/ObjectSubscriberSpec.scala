/*
* Copyright (C) 2014-2015 Really Inc. <http://really.io>
*/

package io.really.gorilla

import akka.actor.Props
import akka.testkit.{ TestProbe, TestActorRef }
import _root_.io.really._
import _root_.io.really.gorilla.SubscriptionManager.{ UpdateSubscriptionFields, Unsubscribe }
import _root_.io.really.protocol.ProtocolFormats.PushMessageWrites.{ Deleted, Created }
import _root_.io.really.gorilla.GorillaEventCenter._
import _root_.io.really.fixture.PersistentModelStoreFixture
import _root_.io.really.model.Model
import _root_.io.really.model.persistent.ModelRegistry.CollectionActorMessage
import _root_.io.really.model.persistent.ModelRegistry.ModelResult
import _root_.io.really.model.persistent.PersistentModelStore
import akka.persistence.{ Update => PersistenceUpdate }
import play.api.libs.json.Json

class ObjectSubscriberSpec extends BaseActorSpecWithMongoDB {

  val caller = TestProbe()
  val requestDelegate = TestProbe()
  val pushChannel = TestProbe()
  val deathProbe = TestProbe()
  val rev: Revision = 1L
  val r: R = R / 'users / 601
  val rSub = RSubscription(ctx, r, None, rev, requestDelegate.ref, pushChannel.ref)
  val userInfo = UserInfo(AuthProvider.Anonymous, R("/_anonymous/1234567"), Application("reallyApp"))
  implicit val session = globals.session

  val userModel = BaseActorSpec.userModel.copy()

  val models: List[Model] = List(BaseActorSpec.userModel, BaseActorSpec.carModel,
    BaseActorSpec.companyModel, BaseActorSpec.authorModel, BaseActorSpec.postModel)

  override def beforeAll() = {
    super.beforeAll()
    globals.persistentModelStore ! PersistentModelStore.UpdateModels(models)
    globals.persistentModelStore ! PersistentModelStoreFixture.GetState
    expectMsg(models)

    globals.modelRegistry ! PersistenceUpdate(await = true)
    globals.modelRegistry ! CollectionActorMessage.GetModel(BaseActorSpec.userModel.r, self)
    expectMsg(ModelResult.ModelObject(BaseActorSpec.userModel, List.empty))
  }

  "Object Subscriber" should "Initialized Successfully" in {
    val rSub1 = RSubscription(ctx, r, Some(Set("name")), rev, requestDelegate.ref, pushChannel.ref)
    val objectSubscriberActor1 = TestActorRef[ObjectSubscriber](globals.objectSubscriberProps(rSub1))
    objectSubscriberActor1.underlyingActor.fields shouldEqual Set("name")
    objectSubscriberActor1.underlyingActor.r shouldEqual r
    objectSubscriberActor1.underlyingActor.logTag shouldEqual s"ObjectSubscriber ${pushChannel.ref.path}$$$r"

    val rSub2 = RSubscription(ctx, r, Some(Set.empty), rev, requestDelegate.ref, pushChannel.ref)
    val objectSubscriberActor2 = TestActorRef[ObjectSubscriber](globals.objectSubscriberProps(rSub2))
    val replayer = system.actorOf(Props(new Replayer(globals, objectSubscriberActor2, rSub2, Some(rev))))
    objectSubscriberActor2.tell(ReplayerSubscribed(replayer), caller.ref)
    Thread.sleep(100)
    objectSubscriberActor2.underlyingActor.fields shouldEqual Set("name", "age")
  }

  it should "handle Unsubscribe successfully during starter receiver and self termination" in {
    val rSub1 = RSubscription(ctx, r, Some(Set("name")), rev, requestDelegate.ref, pushChannel.ref)
    val objectSubscriberActor = TestActorRef[ObjectSubscriber](globals.objectSubscriberProps(rSub1))
    deathProbe.watch(objectSubscriberActor)
    objectSubscriberActor.underlyingActor.fields shouldEqual Set("name")
    objectSubscriberActor.tell(SubscriptionManager.Unsubscribe, caller.ref)
    requestDelegate.expectMsg(SubscriptionManager.Unsubscribe)
    deathProbe.expectTerminated(objectSubscriberActor)
  }

  it should "handle Unsubscribe during withModel and self termination" in {
    val rSub = RSubscription(ctx, r, Some(Set.empty), rev, requestDelegate.ref, pushChannel.ref)
    val objectSubscriberActor = TestActorRef[ObjectSubscriber](globals.objectSubscriberProps(rSub))
    deathProbe.watch(objectSubscriberActor)
    val replayer = system.actorOf(Props(new Replayer(globals, objectSubscriberActor, rSub, Some(rev))))
    objectSubscriberActor.tell(ReplayerSubscribed(replayer), caller.ref)
    Thread.sleep(100)
    objectSubscriberActor.underlyingActor.fields shouldEqual Set("name", "age") //Model has been retrieved
    objectSubscriberActor.tell(Unsubscribe, caller.ref)
    requestDelegate.expectMsg(Unsubscribe)
    deathProbe.expectTerminated(objectSubscriberActor)
  }

  it should "update Internal field List Successfully" in {
    val rSub1 = RSubscription(ctx, r, Some(Set("name")), rev, requestDelegate.ref, pushChannel.ref)
    val objectSubscriberActor = TestActorRef[ObjectSubscriber](globals.objectSubscriberProps(rSub1))
    val replayer = system.actorOf(Props(new Replayer(globals, objectSubscriberActor, rSub1, Some(rev))))
    objectSubscriberActor.tell(ReplayerSubscribed(replayer), caller.ref)
    Thread.sleep(100)
    objectSubscriberActor.tell(UpdateSubscriptionFields(Set("age")), caller.ref)
    objectSubscriberActor.underlyingActor.fields shouldEqual Set("name", "age")
  }

  it should "pass delete updates to push channel actor correctly and then terminates" in {
    val rSub1 = RSubscription(ctx, r, Some(Set("name")), rev, requestDelegate.ref, pushChannel.ref)
    val objectSubscriberActor = TestActorRef[ObjectSubscriber](globals.objectSubscriberProps(rSub1))
    val replayer = system.actorOf(Props(new Replayer(globals, objectSubscriberActor, rSub1, Some(rev))))
    objectSubscriberActor.tell(ReplayerSubscribed(replayer), caller.ref)
    caller.expectNoMsg()
    objectSubscriberActor.underlyingActor.fields shouldEqual Set("name")
    objectSubscriberActor ! GorillaLogDeletedEntry(r, rev, 1l, userInfo)
    pushChannel.expectMsg(Deleted.toJson(userInfo.userR, r))
    deathProbe.watch(objectSubscriberActor)
    deathProbe.expectTerminated(objectSubscriberActor)
  }

  //  it should "pass created updates to push channel correctly" in {
  //    val rSub = RSubscription(ctx, r, Some(Set("name")), rev, requestDelegate.ref, pushChannel.ref)
  //    val objectSubscriberActor = TestActorRef[ObjectSubscriber](globals.objectSubscriberProps(rSub))
  //    val replayer = system.actorOf(Props(new Replayer(globals, objectSubscriberActor, rSub, Some(rev))))
  //    objectSubscriberActor.tell(ReplayerSubscribed(replayer), caller.ref)
  //    caller.expectNoMsg()
  //    objectSubscriberActor.underlyingActor.fields shouldEqual Set("name")
  //    objectSubscriberActor ! GorillaLogCreatedEntry(r, Json.obj(), rev, 23l, userInfo)
  //    pushChannel.expectMsg(Created.toJson(r, Json.obj()))
  //  }

  it should "filter the hidden fields from the an empty list subscription and sent the rest of model fields" in {}

  it should "filter the hidden fields from the the subscription list and sent the rest of the subscription list " in {}

  it should "pass nothing if the model.executeOnGet evaluated to Terminated" in {}

  it should "in case of subscription failure, log and acknowledge the delegate and then stop" in {}

  it should "handle associated replayer termination" in {}

  it should "handle ModelUpdated correctly" in {}

  it should "handle ModelDeleted, send subscription failed and terminates" in {}

  it should "handle if the pushed update model version is not equal to the state version" in {}

  it should "suicide if the associated Replayer did not send a ReplayerSubscribed for a configurable time" in {}

}
