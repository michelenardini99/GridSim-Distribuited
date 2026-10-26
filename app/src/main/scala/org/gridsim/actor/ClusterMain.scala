package org.gridsim.actor

import java.net.InetAddress
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.cluster.sharding.typed.scaladsl.{ClusterSharding, Entity}
import org.apache.pekko.cluster.typed.{ClusterSingleton, SingletonActor}
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.persistence.typed.PersistenceId
import org.gridsim.actor.telemetry.{
  KafkaEntityTelemetryPublisher,
  KafkaSimulationControlPublisher,
  KafkaSimulationTickPublisher,
  TelemetryPublisher
}
import org.gridsim.core.behaviour.{
  EntityEvolutionDispatcher,
  EntityEvolutionHandler,
  EvolutionRequest
}
import org.gridsim.core.behaviour.house.ConsumptionResolver.given
import org.gridsim.core.behaviour.shaping.DemandShaper.default
import org.gridsim.core.behaviour.house.HouseEvolutionDependencies.given
import org.gridsim.core.common.{Energy, Flow}
import org.gridsim.core.model.{GridEntity, GridEntityState}
import com.typesafe.config.ConfigFactory

import scala.concurrent.duration.*
import scala.util.{Failure, Success, Try}

/** Dedicated entrypoint for running the simulation backend in a replicated
  * containerized environment (e.g. Docker Compose) with automatic master
  * election via Pekko Cluster and ClusterSingleton.
  */
object ClusterMain:

  private val SystemName = "GridSimAgentSystem"

  private def resolveSeedNodes(
      serviceNameOpt: Option[String],
      explicitSeedsOpt: Option[String],
      selfHost: String,
      port: Int
  ): List[String] =
    explicitSeedsOpt.filter(_.trim.nonEmpty) match
      case Some(seeds) =>
        seeds.split(",").map(_.trim).filter(_.nonEmpty).toList
      case None =>
        serviceNameOpt.filter(_.trim.nonEmpty) match
          case Some(serviceName) =>
            // Retry DNS resolution briefly to allow sibling replicas to register in Docker DNS
            var discoveredIps: List[String] = Nil
            var attempts = 0
            while attempts < 5 && discoveredIps.size < 2 do
              discoveredIps = Try(
                InetAddress
                  .getAllByName(serviceName)
                  .map(_.getHostAddress)
                  .distinct
                  .sorted
                  .toList
              ).getOrElse(Nil)
              if discoveredIps.size < 2 then
                Thread.sleep(500)
              attempts += 1

            val allIps =
              if discoveredIps.nonEmpty then
                (discoveredIps :+ selfHost).distinct.sorted
              else List(selfHost)

            allIps.map(ip => s"pekko://$SystemName@$ip:$port")
          case None =>
            List(s"pekko://$SystemName@$selfHost:$port")

  def main(args: Array[String]): Unit =
    val defaultLocalIp =
      Try(InetAddress.getLocalHost.getHostAddress).getOrElse("127.0.0.1")
    val hostname = sys.env.getOrElse("PEKKO_HOSTNAME", defaultLocalIp)
    val port =
      sys.env.get("PEKKO_PORT").flatMap(_.toIntOption).getOrElse(2551)
    val httpPort =
      sys.env.get("HTTP_PORT").flatMap(_.toIntOption).getOrElse(8080)
    val kafkaServers =
      sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    val redisHost = sys.env.getOrElse("REDIS_HOST", "localhost")
    val redisPort =
      sys.env.get("REDIS_PORT").flatMap(_.toIntOption).getOrElse(6379)
    val serviceName = sys.env.get("CLUSTER_SERVICE_NAME")
    val explicitSeeds = sys.env.get("PEKKO_SEED_NODES")

    val seedNodes =
      resolveSeedNodes(serviceName, explicitSeeds, hostname, port)
    val seedNodesHocon = seedNodes.map(s => s""""$s"""").mkString("[", ", ", "]")

    val config = ConfigFactory
      .parseString(s"""
      pekko.actor.provider = cluster
      pekko.actor.allow-java-serialization = on
      pekko.actor.warn-about-java-serializer-usage = off
      pekko.remote.artery.canonical.hostname = "$hostname"
      pekko.remote.artery.canonical.port = $port
      pekko.cluster.seed-nodes = $seedNodesHocon
      pekko.persistence.journal.plugin = "gridsim.persistence.redis.journal"
      pekko.persistence.snapshot-store.plugin = "gridsim.persistence.redis.snapshot"
      gridsim.persistence.redis.journal {
        class = "org.gridsim.actor.persistence.RedisJournal"
        plugin-dispatcher = "pekko.actor.default-dispatcher"
        host = "$redisHost"
        port = $redisPort
      }
      gridsim.persistence.redis.snapshot {
        class = "org.gridsim.actor.persistence.RedisSnapshotStore"
        plugin-dispatcher = "pekko.actor.default-dispatcher"
        host = "$redisHost"
        port = $redisPort
      }
      pekko.cluster.downing-provider-class = "org.apache.pekko.cluster.sbr.SplitBrainResolverProvider"
      pekko.cluster.split-brain-resolver.active-strategy = keep-majority
      pekko.cluster.split-brain-resolver.stable-after = 10s
    """)
      .withFallback(ConfigFactory.load())

    ActorSystem[Nothing](
      Behaviors.setup[Nothing] { context =>
        implicit val system = context.system
        val sharding = ClusterSharding(system)
        val singletonManager = ClusterSingleton(system)

        context.log.info(
          s"Starting GridSim cluster node at $hostname:$port with seeds: ${seedNodes.mkString(", ")}"
        )

        // Initialize Default Dispatcher
        val dispatcher = EntityEvolutionDispatcher.default
        val handlerFor = (_: GridEntity) =>
          new EntityEvolutionHandler:
            override def supports(req: EvolutionRequest): Boolean = true
            override def evolve(
                req: EvolutionRequest
            ): (GridEntityState, Flow[Energy]) = dispatcher.evolve(req)

        val kafkaProducer = TelemetryPublisher.createKafkaProducer(kafkaServers)

        // 1. Initialize EntityActor Sharding
        sharding.init(Entity(EntityActor.TypeKey) { entityContext =>
          val entityId = entityContext.entityId
          val simulationId =
            if entityId.length > 36 then entityId.take(36) else entityId

          EntityActor(
            PersistenceId(entityContext.entityTypeKey.name, entityId),
            handlerFor,
            new KafkaEntityTelemetryPublisher(
              kafkaProducer,
              s"grid.entities.$simulationId"
            )
          )
        })

        // 2. Initialize SimulationActor Sharding (rememberEntities = true so active simulations automatically respawn on rebalance/failover)
        sharding.init(
          Entity(SimulationActor.TypeKey) { entityContext =>
            val simulationId = entityContext.entityId
            val entityRefFor = (localId: String) =>
              sharding.entityRefFor(
                EntityActor.TypeKey,
                EntityActor.entityId(simulationId, localId)
              )

            SimulationActor(
              PersistenceId(entityContext.entityTypeKey.name, simulationId),
              entityRefFor,
              1.second,
              new KafkaSimulationTickPublisher(
                kafkaProducer,
                s"grid.ticks.$simulationId"
              ),
              new KafkaSimulationControlPublisher(
                kafkaProducer,
                simulationId,
                s"grid.control.$simulationId"
              )
            )
          }.withSettings(
            org.apache.pekko.cluster.sharding.typed.ClusterShardingSettings(system)
              .withRememberEntities(true)
          )
        )

        // 3. Elect / Connect to the Master SimulationRegistryActor via ClusterSingleton (event-sourced in Redis)
        val registryProxy = singletonManager.init(
          SingletonActor(SimulationRegistryActor.persistent(), "SimulationRegistry")
        )

        // 4. Start HTTP Server on every replica (routed via ClusterSingleton and ClusterSharding)
        val routes = new SimulationControlRoutes(
          registryProxy,
          (id: String) => sharding.entityRefFor(SimulationActor.TypeKey, id)
        ).routes

        val bindingFuture = Http().newServerAt("0.0.0.0", httpPort).bind(routes)

        implicit val ec = system.executionContext
        bindingFuture.onComplete {
          case Success(_) =>
            context.log.info(
              s"Cluster Simulation API Server online at http://$hostname:$httpPort/"
            )
          case Failure(ex) =>
            context.log.error(
              "Failed to bind HTTP endpoint, terminating system",
              ex
            )
            system.terminate()
        }

        Behaviors.empty
      },
      SystemName,
      config
    )

