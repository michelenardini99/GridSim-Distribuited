package org.gridsim.actor

import org.apache.pekko.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import org.apache.pekko.persistence.testkit.scaladsl.EventSourcedBehaviorTestKit
import org.apache.pekko.persistence.typed.PersistenceId
import org.gridsim.actor.EntityActor
import org.gridsim.actor.EntityActor.*
import org.gridsim.core.behaviour.{EntityEvolutionHandler, EvolutionRequest}
import org.gridsim.core.common.*
import org.gridsim.core.common.Energy.*
import org.gridsim.core.common.Flow.*
import org.gridsim.actor.protocol.EntityProtocol.*
import org.gridsim.core.model.{Environment, GridEntity, GridEntityState, WeatherConditions}
import org.junit.runner.RunWith
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.junit.JUnitRunner

import java.time.LocalDateTime
import java.util.UUID
import scala.concurrent.duration.*

@RunWith(classOf[JUnitRunner])
class EntityActorSpec
    extends ScalaTestWithActorTestKit(EventSourcedBehaviorTestKit.config)
    with AnyFlatSpecLike
    with Matchers {

  private case class TestEntity(id: String) extends GridEntity
  private case class TestEntityState(entityId: String, value: Int = 0) extends GridEntityState

  private def testEnvironment(t: FiniteDuration = 0.seconds): Environment = new Environment:
    override def startDateTime: LocalDateTime = LocalDateTime.now
    override def time: FiniteDuration = t
    override def weather(point: GeographicPoint): WeatherConditions = ???
    override def advance(delta: FiniteDuration): Environment = ???

  private class RecordingHandler(nextState: GridEntityState, flow: Flow[Energy]) extends EntityEvolutionHandler:
    var lastRequest: Option[EvolutionRequest] = None

    override def supports(request: EvolutionRequest): Boolean = true

    override def evolve(request: EvolutionRequest): (GridEntityState, Flow[Energy]) =
      lastRequest = Some(request)
      (nextState, flow)

  /** Handler stub whose result depends on the call count, used to prove that the actor
    * threads the previous result back into the next request instead of the initial state. */
  private class SequenceHandler(results: Seq[GridEntityState]) extends EntityEvolutionHandler:
    private var callCount = 0
    var stateSeenOnSecondCall: Option[GridEntityState] = None

    override def supports(request: EvolutionRequest): Boolean = true

    override def evolve(request: EvolutionRequest): (GridEntityState, Flow[Energy]) =
      callCount += 1
      if callCount == 2 then stateSeenOnSecondCall = Some(request.state)
      (results(callCount - 1), Flow.Balanced)

  /** Every test gets its own persistence id so tests don't share journal/snapshot storage. */
  private def newPersistenceId(): PersistenceId =
    PersistenceId(EntityActor.TypeKey.name, UUID.randomUUID().toString)

  private def kitFor(
    handler: EntityEvolutionHandler,
    persistenceId: PersistenceId = newPersistenceId(),
    publisher: org.gridsim.actor.telemetry.EntityTelemetryPublisher = org.gridsim.actor.telemetry.TelemetryPublisher.NoOpEntityTelemetryPublisher
  ): EventSourcedBehaviorTestKit[EntityCommand, EntityEvent, State] =
    EventSourcedBehaviorTestKit[EntityCommand, EntityEvent, State](
      testKit.system,
      EntityActor(persistenceId, _ => handler, publisher),
      EventSourcedBehaviorTestKit.SerializationSettings.disabled
    )

  "EntityActor" should "reject Evolve before being initialized, without persisting anything or replying" in {
    val handler = RecordingHandler(TestEntityState("E0"), Flow.Balanced)
    val kit = kitFor(handler)
    val probe = testKit.createTestProbe[EntityEvolved]()

    val result = kit.runCommand(Evolve(testEnvironment(), 1.hour, probe.ref))

    result.hasNoEvents shouldBe true
    probe.expectNoMessage()
    kit.getState() shouldBe State(None, None)
  }

  it should "persist Initialized and reply with Ack the first time it is initialized" in {
    val entity = TestEntity("E1")
    val initialState = TestEntityState("E1", value = 0)
    val handler = RecordingHandler(initialState, Flow.Balanced)
    val kit = kitFor(handler)

    val result = kit.runCommand[Ack.type](replyTo => Initialize(entity, Some(initialState), replyTo))

    result.reply shouldBe Ack
    result.event shouldBe Initialized(entity, Some(initialState))
    result.state shouldBe State(Some(entity), Some(initialState))
  }

  it should "ignore a second Initialize once the entity is already initialized" in {
    val entity = TestEntity("E5")
    val initialState = TestEntityState("E5", value = 1)
    val handler = RecordingHandler(initialState, Flow.Balanced)
    val kit = kitFor(handler)

    kit.runCommand[Ack.type](replyTo => Initialize(entity, Some(initialState), replyTo))

    val otherEntity = TestEntity("other")
    val otherState = TestEntityState("other", value = 99)
    val probe = testKit.createTestProbe[Ack.type]()
    val result = kit.runCommand(Initialize(otherEntity, Some(otherState), probe.ref))

    result.hasNoEvents shouldBe true
    probe.expectNoMessage()
    kit.getState() shouldBe State(Some(entity), Some(initialState))
  }


  it should "persist Evolved and reply with EntityEvolved carrying the state and flow produced by the handler" in {
    val entity = TestEntity("E2")
    val initialState = TestEntityState("E2", value = 0)
    val evolvedState = TestEntityState("E2", value = 1)
    val handler = RecordingHandler(evolvedState, Surplus(2.kwh))
    val kit = kitFor(handler)

    kit.runCommand[Ack.type](replyTo => Initialize(entity, Some(initialState), replyTo))
    val result = kit.runCommand[EntityEvolved](replyTo => Evolve(testEnvironment(), 1.hour, replyTo))

    result.reply shouldBe EntityEvolved(entity.id, Surplus(2.kwh))
    result.event shouldBe Evolved(evolvedState, Surplus(2.kwh))
    result.state shouldBe State(Some(entity), Some(evolvedState))
  }

  it should "forward entity, current state, environment and delta to the handler unchanged" in {
    val entity = TestEntity("E3")
    val initialState = TestEntityState("E3", value = 42)
    val handler = RecordingHandler(initialState, Flow.Balanced)
    val env = testEnvironment(3.hours)
    val delta = 30.minutes
    val kit = kitFor(handler)

    kit.runCommand[Ack.type](replyTo => Initialize(entity, Some(initialState), replyTo))
    kit.runCommand[EntityEvolved](replyTo => Evolve(env, delta, replyTo))

    handler.lastRequest shouldBe Some(EvolutionRequest(entity, initialState, env, delta))
  }

  it should "carry the updated state forward to the next Evolve, not the initial one" in {
    val entity = TestEntity("E4")
    val initialState = TestEntityState("E4", value = 0)
    val afterFirst = TestEntityState("E4", value = 1)
    val afterSecond = TestEntityState("E4", value = 2)
    val handler = SequenceHandler(Seq(afterFirst, afterSecond))
    val kit = kitFor(handler)

    kit.runCommand[Ack.type](replyTo => Initialize(entity, Some(initialState), replyTo))

    val first = kit.runCommand[EntityEvolved](replyTo => Evolve(testEnvironment(), 1.hour, replyTo, 0L))
    first.reply shouldBe EntityEvolved(entity.id, Flow.Balanced)

    val second = kit.runCommand[EntityEvolved](replyTo => Evolve(testEnvironment(), 1.hour, replyTo, 1L))
    second.reply shouldBe EntityEvolved(entity.id, Flow.Balanced)

    handler.stateSeenOnSecondCall shouldBe Some(afterFirst)
  }

  it should "report a deficit flow returned by the handler as-is" in {
    val entity = TestEntity("E6")
    val initialState = TestEntityState("E6")
    val handler = RecordingHandler(initialState, Deficit(1.5.kwh))
    val kit = kitFor(handler)

    kit.runCommand[Ack.type](replyTo => Initialize(entity, Some(initialState), replyTo))
    val result = kit.runCommand[EntityEvolved](replyTo => Evolve(testEnvironment(), 1.hour, replyTo))

    result.reply shouldBe EntityEvolved(entity.id, Deficit(1.5.kwh))
  }

  it should "report a balanced flow returned by the handler as-is" in {
    val entity = TestEntity("E7")
    val initialState = TestEntityState("E7")
    val handler = RecordingHandler(initialState, Flow.Balanced)
    val kit = kitFor(handler)

    kit.runCommand[Ack.type](replyTo => Initialize(entity, Some(initialState), replyTo))
    val result = kit.runCommand[EntityEvolved](replyTo => Evolve(testEnvironment(), 1.hour, replyTo))

    result.reply shouldBe EntityEvolved(entity.id, Flow.Balanced)
  }

  it should "publish telemetry directly to the EntityTelemetryPublisher when evolving" in {
    val entity = TestEntity("E_TELEMETRY")
    val initialState = TestEntityState("E_TELEMETRY", value = 10)
    val evolvedState = TestEntityState("E_TELEMETRY", value = 20)
    val handler = RecordingHandler(evolvedState, Surplus(5.kwh))
    val publisher = new org.gridsim.actor.telemetry.TelemetryPublisher.RecordingEntityTelemetryPublisher()
    val kit = kitFor(handler, publisher = publisher)

    kit.runCommand[Ack.type](replyTo => Initialize(entity, Some(initialState), replyTo))
    kit.runCommand[EntityEvolved](replyTo => Evolve(testEnvironment(), 15.minutes, replyTo, 42L))

    publisher.records should have size 1
    val record = publisher.records.head
    record.entityId shouldBe "E_TELEMETRY"
    record.tick shouldBe 42L
    record.state shouldBe evolvedState
    record.flow shouldBe Surplus(5.kwh)
  }


  it should "restore the last persisted state after a restart (event replay)" in {
    val entity = TestEntity("E8")
    val initialState = TestEntityState("E8", value = 0)
    val afterEvolve = TestEntityState("E8", value = 5)
    val handler = RecordingHandler(afterEvolve, Flow.Balanced)
    val kit = kitFor(handler)

    kit.runCommand[Ack.type](replyTo => Initialize(entity, Some(initialState), replyTo))
    kit.runCommand[EntityEvolved](replyTo => Evolve(testEnvironment(), 1.hour, replyTo))

    val restarted = kit.restart()

    restarted.state shouldBe State(Some(entity), Some(afterEvolve))
  }

  it should "persist a snapshot once the configured retention threshold (100 persisted events) is reached" in {
    val entity = TestEntity("E9")
    val initialState = TestEntityState("E9", value = 0)
    // 1 Initialize + 99 Evolve = 100 persisted events, matching RetentionCriteria.snapshotEvery(100, ...)
    val evolvedStates = (1 to 99).map(i => TestEntityState("E9", value = i))
    val handler = SequenceHandler(evolvedStates)
    val persistenceId = newPersistenceId()
    val kit = kitFor(handler, persistenceId)

    kit.runCommand[Ack.type](replyTo => Initialize(entity, Some(initialState), replyTo))
    evolvedStates.indices.foreach { _ =>
      kit.runCommand[EntityEvolved](replyTo => Evolve(testEnvironment(), 1.hour, replyTo))
    }

    val snapshotKit = kit.snapshotTestKit.getOrElse(
      fail("EventSourcedBehaviorTestKit.config is expected to configure an in-memory snapshot store")
    )
    snapshotKit.expectNextPersisted(persistenceId.id, State(Some(entity), Some(evolvedStates.last)))
  }
}
