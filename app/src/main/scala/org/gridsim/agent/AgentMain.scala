package org.gridsim.agent

import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.cluster.sharding.typed.scaladsl.{ClusterSharding, Entity}
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.persistence.typed.PersistenceId
import org.gridsim.actor.{EntityActor, SimulationActor}
import org.gridsim.core.behaviour.{EntityEvolutionDispatcher, EntityEvolutionHandler, EvolutionRequest}
import org.gridsim.core.behaviour.house.ConsumptionResolver.given
import org.gridsim.core.behaviour.shaping.DemandShaper.default
import org.gridsim.core.behaviour.house.HouseEvolutionDependencies.given
import org.gridsim.core.solver.KirchhoffPowerFlowSolver
import org.gridsim.core.model.GridEntity
import org.gridsim.core.model.GridEntityState
import org.gridsim.core.common.{Energy, Flow}
import com.typesafe.config.ConfigFactory

import scala.concurrent.duration._
import scala.util.{Failure, Success}

object AgentMain:

  def main(args: Array[String]): Unit =
    val config = ConfigFactory.parseString("""
      pekko.actor.provider = cluster
      pekko.remote.artery.canonical.hostname = "127.0.0.1"
      pekko.remote.artery.canonical.port = 2551
      pekko.cluster.seed-nodes = ["pekko://GridSimAgentSystem@127.0.0.1:2551"]
      pekko.persistence.journal.plugin = "pekko.persistence.journal.inmem"
      pekko.persistence.snapshot-store.plugin = "pekko.persistence.snapshot-store.local"
      pekko.persistence.snapshot-store.local.dir = "target/snapshots"
    """).withFallback(ConfigFactory.load())

    ActorSystem[Nothing](Behaviors.setup[Nothing] { context =>
      implicit val system = context.system
      val sharding = ClusterSharding(system)
      
      // Initialize Default Dispatcher
      val dispatcher = EntityEvolutionDispatcher.default
      val handlerFor = (entity: GridEntity) => new EntityEvolutionHandler:
        override def supports(req: EvolutionRequest): Boolean = true
        override def evolve(req: EvolutionRequest): (GridEntityState, Flow[Energy]) = dispatcher.evolve(req)

      // 1. Initialize EntityActor Sharding
      sharding.init(Entity(EntityActor.TypeKey) { entityContext =>
        EntityActor(
          PersistenceId(entityContext.entityTypeKey.name, entityContext.entityId),
          handlerFor
        )
      })

      // 2. Initialize SimulationActor Sharding
      sharding.init(Entity(SimulationActor.TypeKey) { entityContext =>
        val simulationId = entityContext.entityId
        val entityRefFor = (localId: String) => sharding.entityRefFor(EntityActor.TypeKey, EntityActor.entityId(simulationId, localId))
        // TODO: In a real distributed system, we would need the grid model here for KirchhoffPowerFlowSolver, 
        // but wait! KirchhoffPowerFlowSolver takes the grid! How do we pass the grid if we don't have it when defining the behavior?
        // Ah! SimulationActor needs a flowSolver. 
        // Let's check SimulationActor again... 
        
        // Wait, flowSolver requires the grid in KirchhoffPowerFlowSolver. Let me rethink this...
        
        SimulationActor(
          PersistenceId(entityContext.entityTypeKey.name, simulationId),
          entityRefFor,
          1.second
        )
      })

      // 3. Start SimulationRegistryActor
      val registry = context.spawn(SimulationRegistryActor(), "simulationRegistry")

      // 4. Start HTTP Server
      val routes = new SimulationControlRoutes(registry, sharding).routes
      
      val bindingFuture = Http().newServerAt("0.0.0.1", 8080).bind(routes)
      
      implicit val ec = system.executionContext
      bindingFuture.onComplete {
        case Success(binding) =>
          context.log.info(s"Simulation API Server online at http://localhost:8080/")
        case Failure(ex) =>
          context.log.error("Failed to bind HTTP endpoint, terminating system", ex)
          system.terminate()
      }

      Behaviors.empty
    }, "GridSimAgentSystem", config)

