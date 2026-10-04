package org.gridsim.actor

import org.apache.pekko.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.cluster.sharding.typed.scaladsl.EntityRef
import org.apache.pekko.cluster.sharding.typed.testkit.scaladsl.TestEntityRef
import org.apache.pekko.persistence.testkit.scaladsl.EventSourcedBehaviorTestKit
import org.apache.pekko.persistence.typed.PersistenceId
import org.gridsim.actor.EntityActor
import org.gridsim.actor.protocol.EntityProtocol.{EntityCommand, EntityEvolved, Evolve}
import org.gridsim.actor.protocol.SimulationProtocol.*
import org.gridsim.actor.telemetry.TelemetryPublisher.RecordingSimulationTickPublisher
import org.gridsim.core.common.*
import org.gridsim.core.common.Energy.*
import org.gridsim.core.common.Flow.*
import org.gridsim.core.common.Power.*
import org.gridsim.core.model.*
import org.gridsim.core.model.network.{Cable, CableConnections, GridGraph}
import org.gridsim.core.simulation.{SimulationConf, SimulationModel, SimulationState}
import org.gridsim.core.solver.PowerFlowSolver
import org.junit.runner.RunWith
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.junit.JUnitRunner

import java.time.LocalDateTime
import java.util.UUID
import scala.concurrent.duration.*

@RunWith(classOf[JUnitRunner])
class SimulationActorSpec
    extends ScalaTestWithActorTestKit(EventSourcedBehaviorTestKit.config)
    with AnyFlatSpecLike
    with Matchers {

  private case class DummyEntity(id: String) extends GridEntity

  private val cable = Cable(CableConnections("n1", "n2"), maxCapacity = 50.kw)
  private val gridGraph = GridGraph(nodes = List(DummyEntity("n1"), DummyEntity("n2")), cables = List(cable))
  private val model = SimulationModel(gridGraph)
  private val env = Environment(LocalDateTime.of(2026, 9, 27, 10, 0, 0), 0.seconds)
  private val conf = SimulationConf(delta = 1.minute)

  private val dummySolver = new PowerFlowSolver:
    override def solve(entityFlowMap: scala.collection.Map[String, Flow[Energy]]): scala.collection.Map[Cable, Energy] =
      Map(cable -> 10.0.kwh)

  private def mockEntityRef(id: String): EntityRef[EntityCommand] =
    TestEntityRef(
      EntityActor.TypeKey,
      id,
      testKit.spawn(Behaviors.ignore[EntityCommand])
    )

  private def newPersistenceId(): PersistenceId =
    PersistenceId(SimulationActor.TypeKey.name, UUID.randomUUID().toString)

  private def kitFor(
    publisher: RecordingSimulationTickPublisher = new RecordingSimulationTickPublisher()
  ): (EventSourcedBehaviorTestKit[SimulationCommand, SimulationEvent, SimulationActor.State], RecordingSimulationTickPublisher) =
    val kit = EventSourcedBehaviorTestKit[SimulationCommand, SimulationEvent, SimulationActor.State](
      testKit.system,
      SimulationActor(
        newPersistenceId(),
        id => mockEntityRef(id),
        1.hour,
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

    val result = kit.runCommand(EntitiesEvolved(newEnv, entityResults))

    result.event shouldBe TickAdvanced(newEnv, 1L)
    result.state.tick shouldBe 1L
    result.state.env shouldBe Some(newEnv)

    publisher.records should have size 1
    val record = publisher.records.head
    record.tick shouldBe 0L
    record.env shouldBe newEnv
    record.cableLoads shouldBe Map(cable -> 10.0.kwh)
    record.delta shouldBe conf.delta
  }

  it should "advance multiple ticks and track tick counter accurately" in {
    val (kit, publisher) = kitFor()
    val initState = SimulationState(env, Map.empty, Map.empty, Map.empty)

    kit.runCommand[Ack.type](replyTo => Initialize(model, initState, conf, replyTo))
    kit.runCommand(Start)

    val env1 = env.advance(conf.delta)
    val entityResults1 = List(EntityEvolved("n1", Surplus(5.kwh)), EntityEvolved("n2", Deficit(5.kwh)))
    kit.runCommand(EntitiesEvolved(env1, entityResults1))

    val env2 = env1.advance(conf.delta)
    val entityResults2 = List(EntityEvolved("n1", Surplus(7.kwh)), EntityEvolved("n2", Deficit(7.kwh)))
    val res2 = kit.runCommand(EntitiesEvolved(env2, entityResults2))

    res2.event shouldBe TickAdvanced(env2, 2L)
    res2.state.tick shouldBe 2L

    publisher.records should have size 2
    publisher.records.map(_.tick) shouldBe List(0L, 1L)

    val pauseRes = kit.runCommand(Pause)
    pauseRes.event shouldBe Paused
    pauseRes.state.status shouldBe SimulationActor.Status.PausedStatus
  }
}
