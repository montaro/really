/*
* Copyright (C) 2014-2015 Really Inc. <http://really.io>
*/

package io.really.gorilla

import akka.actor.{Terminated, Props, ActorSystem}
import akka.testkit.{TestProbe, TestActorRef}
import _root_.io.really._
import _root_.io.really.gorilla.SubscriptionManager.{UpdateSubscriptionFields, Unsubscribe}
import _root_.io.really.protocol.ProtocolFormats.PushMessageWrites.Deleted
import _root_.io.really.gorilla.SubscribeAggregator.Subscribed
import _root_.io.really.gorilla.GorillaEventCenter._
import _root_.io.really.fixture.PersistentModelStoreFixture
import _root_.io.really.model.Model
import _root_.io.really.model.persistent.ModelRegistry.CollectionActorMessage
import _root_.io.really.model.persistent.ModelRegistry.ModelResult
import _root_.io.really.model.persistent.PersistentModelStore
import akka.persistence.{Update => PersistenceUpdate}

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

  "Object Subscriber" should "Initialized Successfully" in {
    val rSub1 = RSubscription(ctx, r, Some(Set("name")), rev, requestDelegate.ref, pushChannel.ref)
    val objectSubscriberActor = TestActorRef[ObjectSubscriber](globals.objectSubscriberProps(rSub1))
    objectSubscriberActor.underlyingActor.fields shouldEqual Set("name")
    //TODO Check for R
    //TODO Check for logTag
    //TODO Check for subscribing on emty lists
  }

  it should "handle Unsubscribe successfully during starter receiver and self termination" in {
    val rSub1 = RSubscription(ctx, r, Some(Set("name")), rev, requestDelegate.ref, pushChannel.ref)
    val objectSubscriberActor = TestActorRef[ObjectSubscriber](globals.objectSubscriberProps(rSub1))
    deathPrope.watch(objectSubscriberActor)
    objectSubscriberActor.underlyingActor.fields shouldEqual Set("name")
    objectSubscriberActor.tell(SubscriptionManager.Unsubscribe, caller.ref)
    requestDelegate.expectMsg(SubscriptionManager.Unsubscribe)
    deathPrope.expectTerminated(objectSubscriberActor)
  }

  it should "handle Unsubscribe during withModel and self termination" in {}

  it should "update Internal field List Successfully" in {
    val rSub1 = RSubscription(ctx, r, Some(Set("name")), rev, requestDelegate.ref, pushChannel.ref)
    val objectSubscriberActor = TestActorRef[ObjectSubscriber](globals.objectSubscriberProps(rSub1))
    val replayer = system.actorOf(Props(new Replayer(globals, objectSubscriberActor, rSub1, Some(rev))))
    objectSubscriberActor.tell(ReplayerSubscribed(replayer), caller.ref)
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
    deathPrope.watch(objectSubscriberActor)
    deathPrope.expectTerminated(objectSubscriberActor)
  }

  it should "for empty list subscription change the internal state from empty list to all model fields" in {}

  it should "log a warning when receiving a create push update" in {}

  it should "filter the hidden fields from the an empty list subscription and sent the rest of model fields" in {}

  it should "filter the hidden fields from the the subscription list and sent the rest of the subscription list " in {}

  it should "pass nothing if the model.executeOnGet evaluated to Terminated" in {}

  it should "in case of subscription failure, log and acknowledge the delegate and then stop" in {}

  it should "handle associated replayer termination" in {}

  it should "handle ModelUpdated correctly" in {}

  it should "handle ModelDeleted, send subscription failed and terminates" in {}

  it should "handle if the pushed update model version is not equal to the state version" in {}

}
