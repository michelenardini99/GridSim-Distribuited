package org.gridsim.actor

import org.apache.pekko.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import org.scalatest.wordspec.AnyWordSpecLike
import org.gridsim.actor.SimulationRegistryActor._
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class SimulationRegistryActorTest extends ScalaTestWithActorTestKit with AnyWordSpecLike {

  "A SimulationRegistryActor" should {

    "register a new simulation" in {
      val registry = testKit.spawn(SimulationRegistryActor())
      val probe = testKit.createTestProbe[Registered]()

      registry ! RegisterSimulation("uuid-123", "default_preset", probe.ref)
      probe.expectMessage(Registered("uuid-123"))
    }

    "unregister an existing simulation" in {
      val registry = testKit.spawn(SimulationRegistryActor())
      
      val regProbe = testKit.createTestProbe[Registered]()
      registry ! RegisterSimulation("uuid-123", "default_preset", regProbe.ref)
      regProbe.expectMessage(Registered("uuid-123"))

      val unregProbe = testKit.createTestProbe[Unregistered]()
      registry ! UnregisterSimulation("uuid-123", unregProbe.ref)
      unregProbe.expectMessage(Unregistered("uuid-123"))
    }

    "return a list of active simulations" in {
      val registry = testKit.spawn(SimulationRegistryActor())
      
      val regProbe = testKit.createTestProbe[Registered]()
      registry ! RegisterSimulation("uuid-1", "preset_1", regProbe.ref)
      registry ! RegisterSimulation("uuid-2", "preset_2", regProbe.ref)
      regProbe.receiveMessages(2)

      val getProbe = testKit.createTestProbe[SimulationsList]()
      registry ! GetSimulations(getProbe.ref)
      
      val response = getProbe.receiveMessage()
      response.simulations should have size 2
      response.simulations should contain allOf (
        SimulationInfo("uuid-1", "preset_1"),
        SimulationInfo("uuid-2", "preset_2")
      )
    }

    "not return unregistered simulations" in {
      val registry = testKit.spawn(SimulationRegistryActor())
      
      val regProbe = testKit.createTestProbe[Registered]()
      registry ! RegisterSimulation("uuid-1", "preset_1", regProbe.ref)
      registry ! RegisterSimulation("uuid-2", "preset_2", regProbe.ref)
      regProbe.receiveMessages(2)

      val unregProbe = testKit.createTestProbe[Unregistered]()
      registry ! UnregisterSimulation("uuid-1", unregProbe.ref)
      unregProbe.expectMessage(Unregistered("uuid-1"))

      val getProbe = testKit.createTestProbe[SimulationsList]()
      registry ! GetSimulations(getProbe.ref)
      
      val response = getProbe.receiveMessage()
      response.simulations should have size 1
      response.simulations.head shouldBe SimulationInfo("uuid-2", "preset_2")
    }

    "persist and recover registered simulations across restarts when using persistent behavior" in {
      val pid = org.apache.pekko.persistence.typed.PersistenceId.ofUniqueId("test-registry-1")
      val config = com.typesafe.config.ConfigFactory.parseString("""
        pekko.persistence.journal.plugin = "pekko.persistence.journal.inmem"
      """).withFallback(testKit.system.settings.config)
      val persistentKit = org.apache.pekko.actor.testkit.typed.scaladsl.ActorTestKit(config)
      try {
        val registry1 = persistentKit.spawn(SimulationRegistryActor.persistent(pid))
        val regProbe = persistentKit.createTestProbe[Registered]()
        registry1 ! RegisterSimulation("uuid-persisted", "solar-farm-grid", regProbe.ref)
        regProbe.expectMessage(Registered("uuid-persisted"))

        persistentKit.stop(registry1)

        // Respawn with same PersistenceId and verify state is recovered
        val registry2 = persistentKit.spawn(SimulationRegistryActor.persistent(pid))
        val getProbe = persistentKit.createTestProbe[SimulationsList]()
        registry2 ! GetSimulations(getProbe.ref)
        getProbe.expectMessage(SimulationsList(List(SimulationInfo("uuid-persisted", "solar-farm-grid"))))
      } finally {
        persistentKit.shutdownTestKit()
      }
    }
  }
}
