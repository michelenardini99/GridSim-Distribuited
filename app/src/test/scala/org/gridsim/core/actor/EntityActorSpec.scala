package org.gridsim.core.actor

import org.apache.pekko.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import org.gridsim.core.actor.EntityActor.{EntityEvolved, Evolve}
import org.gridsim.core.behaviour.{EntityEvolutionHandler, EvolutionRequest}
import org.gridsim.core.common.*
import org.gridsim.core.common.Energy.*
import org.gridsim.core.common.Flow.*
import org.gridsim.core.model.{Environment, GridEntity, GridEntityState, WeatherConditions}
import org.junit.runner.RunWith
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.junit.JUnitRunner

import java.time.LocalDateTime
import scala.concurrent.duration.*

@RunWith(classOf[JUnitRunner])
class EntityActorSpec extends ScalaTestWithActorTestKit with AnyFlatSpecLike with Matchers {
  
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

  "EntityActor" should "reply with EntityEvolved carrying the state and flow produced by the handler" in {
    val entity = TestEntity("E1")
    val initialState = TestEntityState("E1", value = 0)
    val evolvedState = TestEntityState("E1", value = 1)
    val handler = RecordingHandler(evolvedState, Surplus(2.kwh))

    val actor = testKit.spawn(EntityActor(initialState, entity, handler))
    val probe = testKit.createTestProbe[EntityEvolved]()

    actor ! Evolve(testEnvironment(), 1.hour, probe.ref)

    probe.expectMessage(EntityEvolved("E1", evolvedState, Surplus(2.kwh)))
  }

  it should "forward entity, current state, environment and delta to the handler unchanged" in {
    val entity = TestEntity("E2")
    val initialState = TestEntityState("E2", value = 42)
    val handler = RecordingHandler(initialState, Flow.Balanced)
    val env = testEnvironment(3.hours)
    val delta = 30.minutes

    val actor = testKit.spawn(EntityActor(initialState, entity, handler))
    val probe = testKit.createTestProbe[EntityEvolved]()

    actor ! Evolve(env, delta, probe.ref)
    probe.expectMessage(EntityEvolved("E2", initialState, Flow.Balanced))

    handler.lastRequest shouldBe Some(EvolutionRequest(entity, initialState, env, delta))
  }

  it should "carry the updated state forward to the next Evolve, not the initial one" in {
    val entity = TestEntity("E3")
    val initialState = TestEntityState("E3", value = 0)
    val afterFirst = TestEntityState("E3", value = 1)
    val afterSecond = TestEntityState("E3", value = 2)

    val handler = SequenceHandler(Seq(afterFirst, afterSecond))

    val actor = testKit.spawn(EntityActor(initialState, entity, handler))
    val probe = testKit.createTestProbe[EntityEvolved]()

    actor ! Evolve(testEnvironment(), 1.hour, probe.ref)
    probe.expectMessage(EntityEvolved("E3", afterFirst, Flow.Balanced))

    actor ! Evolve(testEnvironment(), 1.hour, probe.ref)
    probe.expectMessage(EntityEvolved("E3", afterSecond, Flow.Balanced))

    handler.stateSeenOnSecondCall shouldBe Some(afterFirst)
  }

  it should "report a deficit flow returned by the handler as-is" in {
    val entity = TestEntity("E4")
    val initialState = TestEntityState("E4")
    val handler = RecordingHandler(initialState, Deficit(1.5.kwh))

    val actor = testKit.spawn(EntityActor(initialState, entity, handler))
    val probe = testKit.createTestProbe[EntityEvolved]()

    actor ! Evolve(testEnvironment(), 1.hour, probe.ref)

    probe.expectMessage(EntityEvolved("E4", initialState, Deficit(1.5.kwh)))
  }
}
