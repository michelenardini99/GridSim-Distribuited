package org.gridsim.agent

import org.apache.pekko.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import org.apache.pekko.http.scaladsl.testkit.ScalatestRouteTest
import org.scalatest.wordspec.AnyWordSpecLike
import org.scalatest.matchers.should.Matchers
import org.apache.pekko.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import org.gridsim.actor.protocol.SimulationProtocol.{SimulationCommand, Start, Pause, Stop}
import org.gridsim.agent.SimulationJsonFormats._
import org.gridsim.agent.SimulationRegistryActor._
import org.apache.pekko.cluster.sharding.typed.scaladsl.EntityRef
import org.gridsim.actor.protocol.SimulationProtocol.SimulationCommand
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class SimulationControlRoutesTest 
    extends AnyWordSpecLike 
    with Matchers 
    with ScalatestRouteTest {

  val testKit = org.apache.pekko.actor.testkit.typed.scaladsl.ActorTestKit()
  implicit val typedSystem: org.apache.pekko.actor.typed.ActorSystem[Nothing] = testKit.system

  override def afterAll(): Unit = {
    testKit.shutdownTestKit()
    super.afterAll()
  }

  "SimulationControlRoutes" should {

    "return a list of simulations (GET /api/simulations)" in {
      val registry = testKit.spawn(SimulationRegistryActor(Map("uuid-1" -> SimulationInfo("uuid-1", "preset-a"))))
      
      // We don't need a real EntityRef for this test since GET doesn't use it
      val routes = new SimulationControlRoutes(registry, _ => null).routes

      Get("/api/simulations") ~> routes ~> check {
        status.isSuccess() shouldBe true
        val res = responseAs[SimulationsList]
        res.simulations should have size 1
        res.simulations.head.id shouldBe "uuid-1"
        res.simulations.head.preset shouldBe "preset-a"
      }
    }

    "return 404 for unknown preset (POST /api/simulations)" in {
      val registry = testKit.spawn(SimulationRegistryActor())
      val routes = new SimulationControlRoutes(registry, _ => null).routes

      Post("/api/simulations", CreateSimulationRequest("non_existent_preset")) ~> routes ~> check {
        status.intValue() shouldBe 404
        val res = responseAs[MessageResponse]
        res.message should include("not found")
      }
    }
  }
}
