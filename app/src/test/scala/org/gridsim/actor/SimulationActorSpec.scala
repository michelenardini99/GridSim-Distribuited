package org.gridsim.actor

import org.apache.pekko.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.cluster.sharding.typed.scaladsl.EntityRef
import org.apache.pekko.cluster.sharding.typed.testkit.scaladsl.TestEntityRef
import org.apache.pekko.persistence.testkit.scaladsl.{EventSourcedBehaviorTestKit, PersistenceTestKit}
import org.apache.pekko.persistence.typed.PersistenceId
import org.gridsim.actor.EntityActor
import org.gridsim.actor.protocol.EntityProtocol.{EntityCommand, EntityEvolved, Evolve}
import org.gridsim.actor.protocol.SimulationProtocol.*
import org.gridsim.actor.telemetry.SimulationControlPublisher
import org.gridsim.actor.telemetry.TelemetryPublisher.RecordingSimulationTickPublisher
import org.gridsim.core.behaviour.{EntityEvolutionHandler, EvolutionRequest}
import org.gridsim.core.observability.serialization.SimulationControlUpdate
import org.gridsim.core.common.*
import org.gridsim.core.common.Energy.*
import org.gridsim.core.common.Flow.*
import org.gridsim.core.common.Power.*
import org.gridsim.core.model.*
import org.gridsim.core.model.network.{Cable, CableConnections, ExternalGrid, GridGraph}
import org.gridsim.core.simulation.{SimulationConf, SimulationModel, SimulationSpeed, SimulationState}
import org.gridsim.core.solver.PowerFlowSolver
import org.junit.runner.RunWith
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.junit.JUnitRunner

import java.time.LocalDateTime
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration.*

@RunWith(classOf[JUnitRunner])
class SimulationActorSpec
    extends ScalaTestWithActorTestKit(EventSourcedBehaviorTestKit.config)
    with AnyFlatSpecLike
    with Matchers {

  private case class DummyEntity(id: String) extends GridEntity

  private val cable = Cable(CableConnections("n1", "n2"), maxCapacity = 50.kw)
  private val cable2 = Cable(CableConnections("n2", "eg"), maxCapacity = 50.kw)
  private val gridGraph = GridGraph(nodes = List(DummyEntity("n1"), DummyEntity("n2"), ExternalGrid("eg")), cables = List(cable, cable2))
  private val model = SimulationModel(gridGraph)
  private val env = Environment(LocalDateTime.of(2026, 9, 27, 10, 0, 0), 0.seconds)
  private val conf = SimulationConf(delta = 1.minute)



  private def mockEntityRef(id: String): EntityRef[EntityCommand] =
    TestEntityRef(
      EntityActor.TypeKey,
      id,
      testKit.spawn(Behaviors.receiveMessage[EntityCommand] {
        case org.gridsim.actor.protocol.EntityProtocol.Initialize(_, _, replyTo) =>
          replyTo ! org.gridsim.actor.protocol.EntityProtocol.Ack
          Behaviors.same
        case _ => Behaviors.same
      })
    )

  /** Like [[mockEntityRef]], but counts every Evolve it receives (without ever replying),
    * so a test can observe that a new batch was attempted without relying on timing alone. */
  private def countingNonRespondingEntityRef(id: String, evolveAttempts: AtomicInteger): EntityRef[EntityCommand] =
    TestEntityRef(
      EntityActor.TypeKey,
      id,
      testKit.spawn(Behaviors.receiveMessage[EntityCommand] {
        case org.gridsim.actor.protocol.EntityProtocol.Initialize(_, _, replyTo) =>
          replyTo ! org.gridsim.actor.protocol.EntityProtocol.Ack
          Behaviors.same
        case _: Evolve =>
          evolveAttempts.incrementAndGet()
          Behaviors.same
        case _ => Behaviors.same
      })
    )

  /** Spawns a real [[EntityActor]] (not a mock) so its persisted events/replies can be
    * observed independently of the orchestrator, to make state divergence visible. */
  private def realEntityRef(id: String, persistenceId: PersistenceId, handler: EntityEvolutionHandler): EntityRef[EntityCommand] =
    TestEntityRef(
      EntityActor.TypeKey,
      id,
      testKit.spawn(EntityActor(persistenceId, _ => handler))
    )

  private case class DummyEntityState(entityId: String, value: Int = 0) extends GridEntityState

  /** Handler that counts how many times it evolved and deterministically bumps the state's value. */
  private class IncrementingHandler extends EntityEvolutionHandler:
    val evolveCount = new AtomicInteger(0)
    override def supports(request: EvolutionRequest): Boolean = true
    override def evolve(request: EvolutionRequest): (GridEntityState, Flow[Energy]) =
      evolveCount.incrementAndGet()
      val current = request.state.asInstanceOf[DummyEntityState]
      (current.copy(value = current.value + 1), Flow.Balanced)

  private def newPersistenceId(): PersistenceId =
    PersistenceId(SimulationActor.TypeKey.name, UUID.randomUUID().toString)

  private def kitFor(
    publisher: RecordingSimulationTickPublisher = new RecordingSimulationTickPublisher(),
    tickTimeout: FiniteDuration = 1.hour,
    entityRefFor: String => EntityRef[EntityCommand] = id => mockEntityRef(id)
  ): (EventSourcedBehaviorTestKit[SimulationCommand, SimulationEvent, SimulationActor.State], RecordingSimulationTickPublisher) =
    val kit = EventSourcedBehaviorTestKit[SimulationCommand, SimulationEvent, SimulationActor.State](
      testKit.system,
      SimulationActor(
        newPersistenceId(),
        entityRefFor,
        tickTimeout,
        publisher
      ),
      EventSourcedBehaviorTestKit.SerializationSettings.disabled
    )
    (kit, publisher)

  "SimulationActor" should "initialize properly and transition to IdleStatus" in {
    val (kit, _) = kitFor()
    val initState = SimulationState(env, Map.empty, Map.empty, Map.empty)

    val result = kit.runCommand[Ack.type](replyTo => Initialize(model, initState, conf, replyTo))

    result.reply shouldBe Ack
    result.event shouldBe Initialized(model, env, conf)
    result.state.status shouldBe SimulationActor.Status.IdleStatus
    result.state.tick shouldBe 0L
  }

  it should "publish global tick data to SimulationTickPublisher when entities evolve" in {
    val (kit, publisher) = kitFor()
    val initState = SimulationState(env, Map.empty, Map.empty, Map.empty)

    kit.runCommand[Ack.type](replyTo => Initialize(model, initState, conf, replyTo))
    kit.runCommand(Start)

    val newEnv = env.advance(conf.delta)
    val entityResults = List(
      EntityEvolved("n1", Surplus(5.kwh)),
      EntityEvolved("n2", Deficit(5.kwh))
    )

    val result = kit.runCommand(EntitiesEvolved(newEnv, entityResults, true))

    result.event shouldBe TickAdvanced(newEnv, 1L)
    result.state.tick shouldBe 1L
    result.state.env shouldBe Some(newEnv)

    publisher.records should have size 1
    val record = publisher.records.head
    record.tick shouldBe 0L
    record.env shouldBe newEnv
    record.cableLoads(cable) shouldBe Energy(5.0)
    record.delta shouldBe conf.delta
  }

  it should "advance multiple ticks and track tick counter accurately" in {
    val (kit, publisher) = kitFor()
    val initState = SimulationState(env, Map.empty, Map.empty, Map.empty)

    kit.runCommand[Ack.type](replyTo => Initialize(model, initState, conf, replyTo))
    kit.runCommand(Start)

    val env1 = env.advance(conf.delta)
    val entityResults1 = List(EntityEvolved("n1", Surplus(5.kwh)), EntityEvolved("n2", Deficit(5.kwh)))
    kit.runCommand(EntitiesEvolved(env1, entityResults1, true))

    val env2 = env1.advance(conf.delta)
    val entityResults2 = List(EntityEvolved("n1", Surplus(7.kwh)), EntityEvolved("n2", Deficit(7.kwh)))
    val res2 = kit.runCommand(EntitiesEvolved(env2, entityResults2, true))

    res2.event shouldBe TickAdvanced(env2, 2L)
    res2.state.tick shouldBe 2L

    publisher.records should have size 2
    publisher.records.map(_.tick) shouldBe List(0L, 1L)

    val pauseRes = kit.runCommand(Pause)
    pauseRes.event shouldBe Paused
    pauseRes.state.status shouldBe SimulationActor.Status.PausedStatus
  }

  it should "stop for good and broadcast the stopped status to clients" in {
    val controlUpdates = scala.collection.mutable.ListBuffer.empty[SimulationControlUpdate]
    val controlPublisher = new SimulationControlPublisher:
      override def publish(update: SimulationControlUpdate): Unit = controlUpdates.synchronized(controlUpdates += update)

    val kit = EventSourcedBehaviorTestKit[SimulationCommand, SimulationEvent, SimulationActor.State](
      testKit.system,
      SimulationActor(newPersistenceId(), id => mockEntityRef(id), 1.hour, controlPublisher = controlPublisher),
      EventSourcedBehaviorTestKit.SerializationSettings.disabled
    )
    val initState = SimulationState(env, Map.empty, Map.empty, Map.empty)

    kit.runCommand[Ack.type](replyTo => Initialize(model, initState, conf, replyTo))
    kit.runCommand(Start)

    val stopRes = kit.runCommand(Stop)
    stopRes.event shouldBe Stopped
    stopRes.state.status shouldBe SimulationActor.Status.StoppedStatus
    controlUpdates.synchronized(controlUpdates.last.status) shouldBe "StoppedStatus"

    // A stopped simulation can no longer be restarted or advanced
    kit.runCommand(Start).hasNoEvents shouldBe true
    kit.runCommand(Step).hasNoEvents shouldBe true
    kit.runCommand(EntitiesEvolved(env.advance(conf.delta), Nil, true)).hasNoEvents shouldBe true
  }

  it should "signal TickFailed without advancing the tick or publishing when entities never respond to Evolve" in {
    val (kit, publisher) = kitFor(tickTimeout = 200.millis)
    val initState = SimulationState(env, Map.empty, Map.empty, Map.empty)

    kit.runCommand[Ack.type](replyTo => Initialize(model, initState, conf, replyTo))
    kit.runCommand(Start)

    Thread.sleep(600)

    val state = kit.getState()
    state.tick shouldBe 0L
    state.env shouldBe Some(env)
    state.status shouldBe SimulationActor.Status.RunningStatus
    publisher.records shouldBe empty
  }

  it should "leave independently-persisted entities ahead of the orchestrator when only part of a batch responds" in {
    val publisher = new RecordingSimulationTickPublisher()
    val n1PersistenceId = PersistenceId(EntityActor.TypeKey.name, UUID.randomUUID().toString)
    val n2PersistenceId = PersistenceId(EntityActor.TypeKey.name, UUID.randomUUID().toString)
    val n1Handler = new IncrementingHandler()
    val n2Handler = new IncrementingHandler()

    val n1Ref = realEntityRef("n1", n1PersistenceId, n1Handler)
    val n2Ref = realEntityRef("n2", n2PersistenceId, n2Handler)
    val stragglerRef = mockEntityRef("eg") // acks Initialize, never responds to Evolve

    def entityRefFor(id: String): EntityRef[EntityCommand] = id match
      case "n1" => n1Ref
      case "n2" => n2Ref
      case _    => stragglerRef

    val (kit, _) = kitFor(publisher, tickTimeout = 200.millis, entityRefFor = entityRefFor)
    val initState = SimulationState(
      env,
      Map("n1" -> DummyEntityState("n1"), "n2" -> DummyEntityState("n2")),
      Map.empty,
      Map.empty
    )

    kit.runCommand[Ack.type](replyTo => Initialize(model, initState, conf, replyTo))
    kit.runCommand(Start)

    Thread.sleep(600)

    val state = kit.getState()
    state.tick shouldBe 0L
    publisher.records shouldBe empty

    val persistenceTestKit = PersistenceTestKit(system)
    persistenceTestKit.persistedInStorage(n1PersistenceId.id) should have size 2 // Initialized + Evolved
    persistenceTestKit.persistedInStorage(n2PersistenceId.id) should have size 2
    n1Handler.evolveCount.get() shouldBe 1
    n2Handler.evolveCount.get() shouldBe 1
  }

  it should "not evolve already-advanced entities again when a stalled batch is retried (idempotent retry)" in {
    val publisher = new RecordingSimulationTickPublisher()
    val n1PersistenceId = PersistenceId(EntityActor.TypeKey.name, UUID.randomUUID().toString)
    val n2PersistenceId = PersistenceId(EntityActor.TypeKey.name, UUID.randomUUID().toString)
    val n1Handler = new IncrementingHandler()
    val n2Handler = new IncrementingHandler()

    val n1Ref = realEntityRef("n1", n1PersistenceId, n1Handler)
    val n2Ref = realEntityRef("n2", n2PersistenceId, n2Handler)
    val stragglerRef = mockEntityRef("eg") // never responds, so every retry keeps failing

    def entityRefFor(id: String): EntityRef[EntityCommand] = id match
      case "n1" => n1Ref
      case "n2" => n2Ref
      case _    => stragglerRef

    // Fast retry interval so the automatic TickFailed -> scheduleOnce(..., TickTimer) loop
    // completes several rounds within the test without an excessively long sleep.
    val fastConf = conf.copy(speed = SimulationSpeed.UltraSpeed)
    val (kit, _) = kitFor(publisher, tickTimeout = 150.millis, entityRefFor = entityRefFor)
    val initState = SimulationState(
      env,
      Map("n1" -> DummyEntityState("n1"), "n2" -> DummyEntityState("n2")),
      Map.empty,
      Map.empty
    )

    kit.runCommand[Ack.type](replyTo => Initialize(model, initState, fastConf, replyTo))
    kit.runCommand(Start)

    // Let the first batch time out and several automatic retries (same currentTick, since
    // TickAdvanced is never persisted) run to completion.
    Thread.sleep(900)

    val persistenceTestKit = PersistenceTestKit(system)
    val n1Events = persistenceTestKit.persistedInStorage(n1PersistenceId.id)
    val n2Events = persistenceTestKit.persistedInStorage(n2PersistenceId.id)

    n1Events.size shouldBe 2 // Initialized + a single Evolved
    n2Events.size shouldBe 2
    n1Handler.evolveCount.get() shouldBe 1
    n2Handler.evolveCount.get() shouldBe 1

    // The orchestrator itself never progresses, since the straggler never responds.
    kit.getState().tick shouldBe 0L
  }

  it should "publish a Degraded control update after repeated TickFailed and clear it once a tick succeeds again" in {
    val controlUpdates = scala.collection.mutable.ListBuffer.empty[SimulationControlUpdate]
    val controlPublisher = new SimulationControlPublisher:
      override def publish(update: SimulationControlUpdate): Unit = controlUpdates.synchronized(controlUpdates += update)

    @volatile var respond = false
    def flakyEntityRef(id: String): EntityRef[EntityCommand] =
      TestEntityRef(
        EntityActor.TypeKey,
        id,
        testKit.spawn(Behaviors.receiveMessage[EntityCommand] {
          case org.gridsim.actor.protocol.EntityProtocol.Initialize(_, _, replyTo) =>
            replyTo ! org.gridsim.actor.protocol.EntityProtocol.Ack
            Behaviors.same
          case Evolve(_, _, replyTo, _) =>
            if respond then replyTo ! EntityEvolved(id, Flow.Balanced)
            Behaviors.same
          case _ => Behaviors.same
        })
      )

    val kit = EventSourcedBehaviorTestKit[SimulationCommand, SimulationEvent, SimulationActor.State](
      testKit.system,
      SimulationActor(
        newPersistenceId(),
        id => flakyEntityRef(id),
        tickTimeout = 100.millis,
        controlPublisher = controlPublisher,
        retryBaseBackoff = 50.millis,
        retryMaxBackoff = 50.millis,
        degradedThreshold = 2
      ),
      EventSourcedBehaviorTestKit.SerializationSettings.disabled
    )
    val initState = SimulationState(env, Map.empty, Map.empty, Map.empty)

    kit.runCommand[Ack.type](replyTo => Initialize(model, initState, conf, replyTo))
    kit.runCommand(Start)

    Thread.sleep(900)
    controlUpdates.synchronized(controlUpdates.toList).exists(_.degraded) shouldBe true

    respond = true
    Thread.sleep(500)

    val updates = controlUpdates.synchronized(controlUpdates.toList)
    updates.last.degraded shouldBe false
    kit.getState().tick shouldBe 1L
  }

  it should "restart with backoff after a TickAdvanced persistence failure and re-arm the TickTimer once recovered" in {
    val publisher = new RecordingSimulationTickPublisher()
    val persistenceId = newPersistenceId()
    val persistenceTestKit = PersistenceTestKit(system)
    val evolveAttempts = new AtomicInteger(0)

    val actorRef = testKit.spawn(
      SimulationActor(persistenceId, id => countingNonRespondingEntityRef(id, evolveAttempts), 1.hour, publisher)
    )
    val initState = SimulationState(env, Map.empty, Map.empty, Map.empty)
    val ackProbe = testKit.createTestProbe[Ack.type]()

    actorRef ! Initialize(model, initState, conf, ackProbe.ref)
    ackProbe.expectMessage(Ack)
    actorRef ! Start

    Thread.sleep(200)
    val baselineAttempts = evolveAttempts.get()
    baselineAttempts shouldBe model.grid.nodes.size

    persistenceTestKit.failNextPersisted(persistenceId.id)

    val newEnv = env.advance(conf.delta)
    val entityResults = List(EntityEvolved("n1", Flow.Surplus(5.kwh)), EntityEvolved("n2", Flow.Deficit(5.kwh)))
    actorRef ! EntitiesEvolved(newEnv, entityResults, true)

    Thread.sleep(600)

    publisher.records shouldBe empty // the failed tick's telemetry publish never ran

    val statusProbe = testKit.createTestProbe[String]()
    actorRef ! org.gridsim.actor.protocol.SimulationProtocol.GetStatus(statusProbe.ref)
    statusProbe.expectMessage("RunningStatus") // recovery restored RunningStatus from Initialized+Started

    evolveAttempts.get() should be > baselineAttempts
  }

  it should "not trigger uncontrolled ticking when TickFailed is received while paused" in {
    val (kit, publisher) = kitFor(tickTimeout = 100.millis)
    val initState = SimulationState(env, Map.empty, Map.empty, Map.empty)
    val fastConf = conf.copy(speed = SimulationSpeed.UltraSpeed)

    kit.runCommand[Ack.type](replyTo => Initialize(model, initState, fastConf, replyTo))
    kit.runCommand(Start)
    kit.runCommand(Pause)

    val result = kit.runCommand(TickFailed(new RuntimeException("boom")))
    result.hasNoEvents shouldBe true

    Thread.sleep(400)

    val state = kit.getState()
    state.status shouldBe SimulationActor.Status.PausedStatus
    state.tick shouldBe 0L
    state.env shouldBe Some(env)
    publisher.records shouldBe empty
  }
}
