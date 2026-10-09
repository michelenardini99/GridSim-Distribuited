package org.gridsim.actor

import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.cluster.sharding.typed.scaladsl.{ClusterSharding, Entity}
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.persistence.typed.PersistenceId
import org.gridsim.actor.{EntityActor, SimulationActor}
import org.gridsim.actor.telemetry.{KafkaEntityTelemetryPublisher, KafkaSimulationTickPublisher, TelemetryPublisher}
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

object ActorMain:

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

      val kafkaProducer = TelemetryPublisher.createKafkaProducer("localhost:9092")

      // 1. Initialize EntityActor Sharding
      sharding.init(Entity(EntityActor.TypeKey) { entityContext =>
        val entityId = entityContext.entityId
        // Assuming simulationId is a UUID (36 chars) followed by "-" and localId
        val simulationId = if (entityId.length > 36) entityId.take(36) else entityId
        
        EntityActor(
          PersistenceId(entityContext.entityTypeKey.name, entityId),
          handlerFor,
          new KafkaEntityTelemetryPublisher(kafkaProducer, s"grid.entities.$simulationId")
        )
      })

      // 2. Initialize SimulationActor Sharding
      sharding.init(Entity(SimulationActor.TypeKey) { entityContext =>
        val simulationId = entityContext.entityId
        val entityRefFor = (localId: String) => sharding.entityRefFor(EntityActor.TypeKey, EntityActor.entityId(simulationId, localId))
        
        SimulationActor(
          PersistenceId(entityContext.entityTypeKey.name, simulationId),
          entityRefFor,
          1.second,
          new KafkaSimulationTickPublisher(kafkaProducer, s"grid.ticks.$simulationId")
        )
      })

      // 3. Start SimulationRegistryActor
      val registry = context.spawn(SimulationRegistryActor(), "simulationRegistry")

      // 4. Start HTTP Server
      val routes = new SimulationControlRoutes(
        registry, 
        (id: String) => sharding.entityRefFor(SimulationActor.TypeKey, id)
      ).routes
      
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

