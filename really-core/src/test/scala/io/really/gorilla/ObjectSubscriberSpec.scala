/*
* Copyright (C) 2014-2015 Really Inc. <http://really.io>
*/

package io.really.gorilla

import akka.actor.{ Terminated, Props, ActorSystem }
import akka.testkit.{ TestProbe, TestActorRef }
import _root_.io.really._
import _root_.io.really.gorilla.SubscriptionManager.{ UpdateSubscriptionFields, Unsubscribe }
import _root_.io.really.protocol.ProtocolFormats.PushMessageWrites.Deleted
import _root_.io.really.gorilla.SubscribeAggregator.Subscribed
import _root_.io.really.gorilla.GorillaEventCenter._
import _root_.io.really.fixture.PersistentModelStoreFixture
import _root_.io.really.model.Model
import _root_.io.really.model.persistent.ModelRegistry.CollectionActorMessage
import _root_.io.really.model.persistent.ModelRegistry.ModelResult
import _root_.io.really.model.persistent.PersistentModelStore
import akka.persistence.{ Update => PersistenceUpdate }

import scala.slick.driver.H2Driver.simple._

class ObjectSubscriberSpec extends BaseActorSpecWithMongoDB {

  val caller = TestProbe()
  val requestDelegate = TestProbe()
  val pushChannel = TestProbe()
  val deathPrope = TestProbe()
  val rev: Revision = 1L
  val r: R = R / 'users / 1
  val rSub = RSubscription(ctx, r, None, rev, requestDelegate.ref, pushChannel.ref)
  val userInfo = UserInfo(AuthProvider.Anonymous, R("/_anonymous/1234567"), Application("reallyApp"))
  implicit val session = globals.session

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

  "Object Subscription" should "Initialized Successfully" in {
    val rSub1 = RSubscription(ctx, r, Some(Set("name")), rev, requestDelegate.ref, pushChannel.ref)
    val objectSubscriberActor = TestActorRef[ObjectSubscriber](globals.objectSubscriberProps(rSub1))
    objectSubscriberActor.underlyingActor.fields shouldEqual Set("name")
  }

  it should "unsubscribe Successfully and self terminated" in {
    val rSub1 = RSubscription(ctx, r, Some(Set("name")), rev, requestDelegate.ref, pushChannel.ref)
    val objectSubscriberActor = TestActorRef[ObjectSubscriber](globals.objectSubscriberProps(rSub1))
    objectSubscriberActor ! Subscribed
    objectSubscriberActor.underlyingActor.fields shouldEqual Set("name")
    objectSubscriberActor.tell(SubscriptionManager.Unsubscribe, caller.ref)
    requestDelegate.expectMsg(SubscriptionManager.Unsubscribe)
    deathPrope.watch(objectSubscriberActor)
    deathPrope.expectTerminated(objectSubscriberActor)
  }

  it should "Update Internal field List Successfully" in {
    val rSub1 = RSubscription(ctx, r, Some(Set("name")), rev, requestDelegate.ref, pushChannel.ref)
    val objectSubscriberActor = TestActorRef[ObjectSubscriber](globals.objectSubscriberProps(rSub1))
    val replayer = system.actorOf(Props(new Replayer(globals, objectSubscriberActor, rSub1, Some(rev))))
    objectSubscriberActor.tell(ReplayerSubscribed(replayer), caller.ref)
    caller.expectNoMsg()
    objectSubscriberActor.tell(UpdateSubscriptionFields(Set("age")), caller.ref)
    caller.expectNoMsg()
    objectSubscriberActor.underlyingActor.fields shouldEqual Set("name", "age")
  }

  it should "pass updates to push channel actor" in {
    val rSub1 = RSubscription(ctx, r, Some(Set("name")), rev, requestDelegate.ref, pushChannel.ref)
    val objectSubscriberActor = TestActorRef[ObjectSubscriber](globals.objectSubscriberProps(rSub1))
    val replayer = system.actorOf(Props(new Replayer(globals, objectSubscriberActor, rSub1, Some(rev))))
    objectSubscriberActor.tell(ReplayerSubscribed(replayer), caller.ref)
    caller.expectNoMsg()
    objectSubscriberActor.underlyingActor.fields shouldEqual Set("name")
    objectSubscriberActor ! GorillaLogDeletedEntry(r, rev, 1l, userInfo)
    pushChannel.expectMsg(Deleted.toJson(userInfo.userR, r))
    deathPrope.watch(objectSubscriberActor)
    deathPrope.expectTerminated(objectSubscriberActor)
  }
}
