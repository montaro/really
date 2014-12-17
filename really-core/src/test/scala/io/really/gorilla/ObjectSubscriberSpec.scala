/*
* Copyright (C) 2014-2015 Really Inc. <http://really.io>
*/

package io.really.gorilla

import akka.actor.{ Terminated, Props, ActorSystem }
import akka.testkit.{ EventFilter, TestProbe, TestActorRef }
import _root_.io.really._
import _root_.io.really.gorilla.SubscriptionManager.{ UpdateSubscriptionFields, Unsubscribe }
import _root_.io.really.protocol.ProtocolFormats.PushMessageWrites.{ Created, Deleted }
import _root_.io.really.gorilla.GorillaEventCenter.ReplayerSubscribed
import _root_.io.really.fixture.PersistentModelStoreFixture
import _root_.io.really.model._
import _root_.io.really.model.persistent.ModelRegistry.CollectionActorMessage
import _root_.io.really.model.persistent.ModelRegistry.ModelResult
import _root_.io.really.model.persistent.PersistentModelStore
import akka.persistence.{ Update => PersistenceUpdate }
import com.typesafe.config.ConfigFactory
import play.api.libs.json._
import scala.slick.driver.H2Driver.simple._

class ObjectSubscriberSpec(config: ReallyConfig) extends BaseActorSpecWithMongoDB(config) {
  def this() = this(new ReallyConfig(ConfigFactory.parseString("""
    really.core.akka.loggers = ["akka.testkit.TestEventListener"],
    really.core.akka.loglevel = WARNING
                                                                """).withFallback(TestConf.getConfig().getRawConfig)))

  val caller = TestProbe()
  val requestDelegate = TestProbe()
  val pushChannel = TestProbe()
  val deathProbe = TestProbe()
  val rev: Revision = 1L
  val r: R = R / 'users / 601
  val rSub = RSubscription(ctx, r, None, rev, requestDelegate.ref, pushChannel.ref)
  val userInfo = UserInfo(AuthProvider.Anonymous, R("/_anonymous/1234567"), Application("reallyApp"))
  implicit val session = globals.session

  //  val userModel = BaseActorSpec.userModel.copy()

  val onGetJs: JsScript =
    """
      |hide("Lname");
    """.stripMargin

  val friendModel = Model(
    R / 'friend,
    CollectionMetadata(23),
    Map(
      "Fname" -> ValueField("Fname", DataType.RString, None, None, true),
      "Lname" -> ValueField("Lname", DataType.RString, None, None, true),
      "age" -> ValueField("age", DataType.RLong, None, None, true)
    ),
    JsHooks(
      None,
      Some(onGetJs),
      None,
      None,
      None,
      None,
      None
    ),
    null,
    List.empty
  )
  val models: List[Model] = List(BaseActorSpec.userModel, BaseActorSpec.carModel,
    BaseActorSpec.companyModel, BaseActorSpec.authorModel, BaseActorSpec.postModel, friendModel)

  override def beforeAll() = {
    super.beforeAll()
    globals.persistentModelStore ! PersistentModelStore.UpdateModels(models)
    globals.persistentModelStore ! PersistentModelStoreFixture.GetState
    expectMsg(models)

    globals.modelRegistry ! PersistenceUpdate(await = true)
    globals.modelRegistry ! CollectionActorMessage.GetModel(BaseActorSpec.userModel.r, self)
    expectMsg(ModelResult.ModelObject(BaseActorSpec.userModel, List.empty))
  }

  //
  //  "Object Subscriber" should "Initialized Successfully" in {
  //    val rSub1 = RSubscription(ctx, r, Some(Set("name")), rev, requestDelegate.ref, pushChannel.ref)
  //    val objectSubscriberActor1 = TestActorRef[ObjectSubscriber](globals.objectSubscriberProps(rSub1))
  //    objectSubscriberActor1.underlyingActor.fields shouldEqual Set("name")
  //    objectSubscriberActor1.underlyingActor.r shouldEqual r
  //    objectSubscriberActor1.underlyingActor.logTag shouldEqual s"ObjectSubscriber ${pushChannel.ref.path}$$$r"
  //
  //    val rSub2 = RSubscription(ctx, r, Some(Set.empty), rev, requestDelegate.ref, pushChannel.ref)
  //    val objectSubscriberActor2 = TestActorRef[ObjectSubscriber](globals.objectSubscriberProps(rSub2))
  //    val replayer = system.actorOf(Props(new Replayer(globals, objectSubscriberActor2, rSub2, Some(rev))))
  //    objectSubscriberActor2.tell(ReplayerSubscribed(replayer), caller.ref)
  //    Thread.sleep(100)
  //    objectSubscriberActor2.underlyingActor.fields shouldEqual Set("name", "age")
  //  }
  //
  //  it should "handle Unsubscribe successfully during starter receiver and self termination" in {
  //    val rSub1 = RSubscription(ctx, r, Some(Set("name")), rev, requestDelegate.ref, pushChannel.ref)
  //    val objectSubscriberActor = TestActorRef[ObjectSubscriber](globals.objectSubscriberProps(rSub1))
  //    deathProbe.watch(objectSubscriberActor)
  //    objectSubscriberActor.underlyingActor.fields shouldEqual Set("name")
  //    objectSubscriberActor.tell(SubscriptionManager.Unsubscribe, caller.ref)
  //    requestDelegate.expectMsg(SubscriptionManager.Unsubscribe)
  //    deathProbe.expectTerminated(objectSubscriberActor)
  //  }
  //
  //  it should "handle Unsubscribe during withModel and self termination" in {
  //    val rSub = RSubscription(ctx, r, Some(Set.empty), rev, requestDelegate.ref, pushChannel.ref)
  //    val objectSubscriberActor = TestActorRef[ObjectSubscriber](globals.objectSubscriberProps(rSub))
  //    deathProbe.watch(objectSubscriberActor)
  //    val replayer = system.actorOf(Props(new Replayer(globals, objectSubscriberActor, rSub, Some(rev))))
  //    objectSubscriberActor.tell(ReplayerSubscribed(replayer), caller.ref)
  //    Thread.sleep(100)
  //    objectSubscriberActor.underlyingActor.fields shouldEqual Set("name", "age") //Model has been retrieved
  //    objectSubscriberActor.tell(Unsubscribe, caller.ref)
  //    requestDelegate.expectMsg(Unsubscribe)
  //    deathProbe.expectTerminated(objectSubscriberActor)
  //  }
  //
  //  it should "update Internal field List Successfully" in {
  //    val rSub1 = RSubscription(ctx, r, Some(Set("name")), rev, requestDelegate.ref, pushChannel.ref)
  //    val objectSubscriberActor = TestActorRef[ObjectSubscriber](globals.objectSubscriberProps(rSub1))
  //    val replayer = system.actorOf(Props(new Replayer(globals, objectSubscriberActor, rSub1, Some(rev))))
  //    objectSubscriberActor ! ReplayerSubscribed(replayer)
  //    Thread.sleep(3000)
  //    objectSubscriberActor ! UpdateSubscriptionFields(Set("age"))
  //    objectSubscriberActor.underlyingActor.fields shouldEqual Set("name", "age")
  //  }
  //
  //  it should "pass delete updates to push channel actor correctly and then terminates" in {
  //    val rSub1 = RSubscription(ctx, r, Some(Set("name")), rev, requestDelegate.ref, pushChannel.ref)
  //    val objectSubscriberActor = TestActorRef[ObjectSubscriber](globals.objectSubscriberProps(rSub1))
  //    val replayer = system.actorOf(Props(new Replayer(globals, objectSubscriberActor, rSub1, Some(rev))))
  //    objectSubscriberActor ! ReplayerSubscribed(replayer)
  //    objectSubscriberActor.underlyingActor.fields shouldEqual Set("name")
  //    objectSubscriberActor ! GorillaLogDeletedEntry(r, rev, 1l, userInfo)
  //    pushChannel.expectMsg(Deleted.toJson(userInfo.userR, r))
  //    deathProbe.watch(objectSubscriberActor)
  //    deathProbe.expectTerminated(objectSubscriberActor)
  //  }
  //
  //  it should "for empty list subscription change the internal state from empty list to all model fields" in {
  //    val rSub = RSubscription(ctx, r, None, rev, requestDelegate.ref, pushChannel.ref)
  //    val objectSubscriber = TestActorRef[ObjectSubscriber](globals.objectSubscriberProps(rSub))
  //    val replayer = system.actorOf(Props(new Replayer(globals, objectSubscriber, rSub, Some(rev))))
  //    objectSubscriber ! ReplayerSubscribed(replayer)
  //    Thread.sleep(300)
  //    objectSubscriber.underlyingActor.fields shouldEqual Set("name", "age")
  //  }

  it should "pass the created event tp push channel actor correctly" in {
    val rSub1 = RSubscription(ctx, r, Some(Set("name", "age")), rev, requestDelegate.ref, pushChannel.ref)
    val objectSubscriberActor = TestActorRef[ObjectSubscriber](globals.objectSubscriberProps(rSub1))
    deathProbe.watch(objectSubscriberActor)
    val replayer = system.actorOf(Props(new Replayer(globals, objectSubscriberActor, rSub1, Some(rev))))
    objectSubscriberActor ! ReplayerSubscribed(replayer)
    val userObject = Json.obj("_r" -> r.toString, "_rev" -> 1L, "name" -> "Hatem", "age" -> 30)
    val createdEvent = GorillaLogCreatedEntry(r, userObject, 1L, 23L, ctx.auth)
    objectSubscriberActor ! createdEvent
    pushChannel.expectMsg(Created.toJson(r, userObject))
  }

  it should "fail with internal server error if the model version was inconsistent" in {
    val rSub1 = RSubscription(ctx, r, None, rev, requestDelegate.ref, pushChannel.ref)
    val objectSubscriberActor = TestActorRef[ObjectSubscriber](globals.objectSubscriberProps(rSub1))
    deathProbe.watch(objectSubscriberActor)
    val replayer = system.actorOf(Props(new Replayer(globals, objectSubscriberActor, rSub1, Some(rev))))
    objectSubscriberActor ! ReplayerSubscribed(replayer)
    val userObject = Json.obj("_r" -> r.toString, "_rev" -> 1L, "name" -> "Hatem", "age" -> 30)
    val createdEvent = GorillaLogCreatedEntry(r, userObject, 1L, 1L, ctx.auth)
    EventFilter.error(occurrences = 1, message = s"ObjectSubscriber ${pushChannel.ref.path}$$$r is going to die since" +
      s" the subscription failed because of: Model Version inconsistency\n error code: 502") intercept {
      objectSubscriberActor ! createdEvent
    }
  }

  it should "filter the hidden fields from the an empty list subscription and sent the rest of model fields" in {
    val r = R / 'friend / 1
    val friendSub = rSub.copy(r = r)
    val objectSubscriber = TestActorRef[ObjectSubscriber](globals.objectSubscriberProps(friendSub))
    val replayer = system.actorOf(Props(new Replayer(globals, objectSubscriber, friendSub, Some(rev))))
    objectSubscriber ! ReplayerSubscribed(replayer)
    Thread.sleep(1000)
    objectSubscriber.underlyingActor.fields shouldEqual Set("Fname", "Lname", "age")
    val friendObject = Json.obj("_r" -> r, "_rev" -> 1L, "Fname" -> "Ahmed", "age" -> 23,
      "Lname" -> "Refaey")
    val createdEvent = GorillaLogCreatedEntry(r, friendObject, 1L, 23L, ctx.auth)
    objectSubscriber ! createdEvent
    pushChannel.expectMsg(Created.toJson(r, friendObject))
  }

  //  it should "filter the hidden fields from the the subscription list and sent the rest of the subscription list " in {
  //    val friendSub = rSub.copy(r = R / 'friend / 1, fields = Some(Set("Fname", "Lname")))
  //    val objectSubscriber2 = TestActorRef[ObjectSubscriber](globals.objectSubscriberProps(friendSub))
  //    val replayer = system.actorOf(Props(new Replayer(globals, objectSubscriber2, friendSub, Some(rev))))
  //    objectSubscriber2.tell(ReplayerSubscribed(replayer), caller.ref)
  //    Thread.sleep(1000)
  //    objectSubscriber2.underlyingActor.fields shouldEqual Set("Fname")
  //  }
  //
  //  ignore should "pass nothing if the model.executeOnGet evaluated to Terminated" in {
  //    //TODO :
  //  }
  //
  //  it should "in case of subscription failure, log and acknowledge the delegate and then stop" in {
  //    val objectSubscriberActor = TestActorRef[ObjectSubscriber](globals.objectSubscriberProps(rSub))
  //    deathProbe.watch(objectSubscriberActor)
  //    EventFilter.error(occurrences = 1, message = s"ObjectSubscriber ${rSub.pushChannel.path}$$${r} is going to die since the subscription failed because of: Internal Server Error\n error code: 401") intercept {
  //      objectSubscriberActor ! SubscriptionFailure(r, 401, "Test Error Reason")
  //    }
  //    pushChannel.expectMsg(SubscriptionFailureWrites.writes(SubscriptionFailure(r, 401, "Internal Server Error")))
  //    deathProbe.expectTerminated(objectSubscriberActor)
  //  }
  //
  //  it should "handle associated replayer termination" in {
  //    val objectSubscriberActor = TestActorRef[ObjectSubscriber](globals.objectSubscriberProps(rSub))
  //    val replayer = system.actorOf(Props(new Replayer(globals, objectSubscriberActor, rSub, Some(rev))))
  //    objectSubscriberActor ! ReplayerSubscribed(replayer)
  //    deathProbe.watch(objectSubscriberActor)
  //    EventFilter.error(occurrences = 1, message = s"ObjectSubscriber ${rSub.pushChannel.path}$$${r} is going to die since the subscription failed because of: Associated replayer stopped\n error code: 505") intercept {
  //      system.stop(replayer)
  //    }
  //    pushChannel.expectMsg(SubscriptionFailureWrites.writes(SubscriptionFailure(r, 505, "Internal Server Error")))
  //    deathProbe.expectTerminated(objectSubscriberActor)
  //  }
  //
  //  it should "handle ModelUpdated correctly" in {
  //    val objectSubscriberActor = TestActorRef[ObjectSubscriber](globals.objectSubscriberProps(rSub))
  //    val replayer = system.actorOf(Props(new Replayer(globals, objectSubscriberActor, rSub, Some(rev))))
  //    objectSubscriberActor ! ReplayerSubscribed(replayer)
  //    objectSubscriberActor ! ModelUpdated(rSub.r.skeleton, BaseActorSpec.userModel, List())
  //    EventFilter.debug(occurrences = 1, message = s"ObjectSubscriber ${rSub.pushChannel.path}$$${r} received a ModelUpdated message for: ${rSub.r}") intercept {
  //      objectSubscriberActor ! ModelUpdated(rSub.r.skeleton, BaseActorSpec.userModel, List())
  //    }
  //  }
  //
  //  it should "handle ModelDeleted, send subscription failed and terminates" in {
  //    val objectSubscriberActor = TestActorRef[ObjectSubscriber](globals.objectSubscriberProps(rSub))
  //    val replayer = system.actorOf(Props(new Replayer(globals, objectSubscriberActor, rSub, Some(rev))))
  //    objectSubscriberActor ! ReplayerSubscribed(replayer)
  //    deathProbe.watch(objectSubscriberActor)
  //    EventFilter.debug(occurrences = 1, message = s"ObjectSubscriber ${rSub.pushChannel.path}$$${r} is going to die since the subscription failed because of: received a DeletedModel message for: $r\n error code: 501") intercept {
  //      objectSubscriberActor ! ModelDeleted(rSub.r.skeleton)
  //    }
  //    pushChannel.expectMsg(SubscriptionFailureWrites.writes(SubscriptionFailure(r, 501, "Internal Server Error")))
  //    deathProbe.expectTerminated(objectSubscriberActor)
  //  }
  //
  //  it should "handle if the pushed update model version is not equal to the state version" in {
  //    val objectSubscriberActor = TestActorRef[ObjectSubscriber](globals.objectSubscriberProps(rSub))
  //    val replayer = system.actorOf(Props(new Replayer(globals, objectSubscriberActor, rSub, Some(rev))))
  //    objectSubscriberActor ! ReplayerSubscribed(replayer)
  //    deathProbe.watch(objectSubscriberActor)
  //    EventFilter.error(occurrences = 1, message = s"ObjectSubscriber ${rSub.pushChannel.path}$$${r} is going to die since the subscription failed because of: Model Version inconsistency\n error code: 502") intercept {
  //      objectSubscriberActor ! GorillaLogUpdatedEntry(rSub.r, Json.obj(), 2L, 1L, ctx.auth, List())
  //    }
  //    pushChannel.expectMsg(SubscriptionFailureWrites.writes(SubscriptionFailure(r, 502, "Internal Server Error")))
  //    deathProbe.expectTerminated(objectSubscriberActor)
  //
  //  }
  //
  //  it should "suicide if the associated Replayer did not send a ReplayerSubscribed for a configurable time" in {}

}
