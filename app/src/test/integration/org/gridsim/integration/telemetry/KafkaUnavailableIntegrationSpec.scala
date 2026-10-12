package org.gridsim.integration.telemetry

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig}
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer, StringDeserializer, StringSerializer}
import org.apache.pekko.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import org.apache.pekko.persistence.testkit.scaladsl.{EventSourcedBehaviorTestKit, PersistenceTestKit}
import org.apache.pekko.persistence.typed.PersistenceId
import org.gridsim.actor.EntityActor
import org.gridsim.actor.protocol.EntityProtocol
import org.gridsim.actor.protocol.EntityProtocol.{EntityCommand, EntityEvent, EntityEvolved, Evolve}
import org.gridsim.actor.telemetry.KafkaEntityTelemetryPublisher
import org.gridsim.core.behaviour.{EntityEvolutionHandler, EvolutionRequest}
import org.gridsim.core.common.*
import org.gridsim.core.common.Energy.*
import org.gridsim.core.common.Flow.*
import org.gridsim.core.model.*
import org.gridsim.core.observability.serialization.SimulationDataCodecs
import org.junit.runner.RunWith
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.junit.JUnitRunner

import java.net.{InetSocketAddress, Socket}
import java.time.LocalDateTime
import java.util.{Collections, Properties, UUID}
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

@RunWith(classOf[JUnitRunner])
class KafkaUnavailableIntegrationSpec

    extends ScalaTestWithActorTestKit(EventSourcedBehaviorTestKit.config)
    with AnyFlatSpecLike
    with Matchers {

  private val BootstrapServers = "localhost:9092"
  private val KafkaContainerName = "gridsim-kafka-test"

  private def isPortOpen(host: String, port: Int, timeoutMs: Int = 1000): Boolean =
    try
      val socket = new Socket()
      socket.connect(new InetSocketAddress(host, port), timeoutMs)
      socket.close()
      true
    catch
      case _: Exception => false

  private def isKafkaReachable(): Boolean = isPortOpen("127.0.0.1", 9092)

  private def runDocker(args: String*): Int =
    val process = new ProcessBuilder((Seq("docker") ++ args)*).redirectErrorStream(true).start()
    process.waitFor()

  private def dockerContainerRunning(name: String): Boolean =
    try runDocker("inspect", "-f", "{{.State.Running}}", name) == 0
    catch case _: Exception => false

  private def stopKafkaContainer(): Unit = runDocker("stop", KafkaContainerName)
  private def startKafkaContainer(): Unit = runDocker("start", KafkaContainerName)

  private def waitForCondition(timeout: FiniteDuration)(condition: => Boolean): Boolean =
    val deadline = System.currentTimeMillis() + timeout.toMillis
    var ok = condition
    while !ok && System.currentTimeMillis() < deadline do
      Thread.sleep(500)
      ok = condition
    ok

  private def createConsumer(): KafkaConsumer[String, Array[Byte]] =
    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BootstrapServers)
    props.put(ConsumerConfig.GROUP_ID_CONFIG, s"test-group-${UUID.randomUUID()}")
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer].getName)
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    new KafkaConsumer[String, Array[Byte]](props)

  private def pollRecords[K, V](
      consumer: KafkaConsumer[K, V],
      expectedCount: Int,
      timeout: FiniteDuration = 10.seconds
  ): List[ConsumerRecord[K, V]] =
    val deadline = System.currentTimeMillis() + timeout.toMillis
    val buffer = collection.mutable.ListBuffer[ConsumerRecord[K, V]]()
    while buffer.size < expectedCount && System.currentTimeMillis() < deadline do
      val polled = consumer.poll(java.time.Duration.ofMillis(200))
      buffer ++= polled.asScala
    buffer.toList

  private case class DummyEntity(id: String) extends GridEntity

  private val env = Environment(LocalDateTime.of(2026, 10, 2, 12, 0, 0), 0.seconds)
  private val delta = 1.minute

  "KafkaEntityTelemetryPublisher" should "block the calling thread for roughly max.block.ms, not fail fast, when the broker has never been reachable" in {
    val maxBlockMs = 2000
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:19999") // nothing listens here
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer].getName)
    props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, maxBlockMs.toString)
    val coldProducer = new KafkaProducer[String, Array[Byte]](props)
    val publisher = new KafkaEntityTelemetryPublisher(coldProducer, "grid.entities.unreachable")

    try
      val start = System.nanoTime()

      noException should be thrownBy
        publisher.publish("solar-1", 0L, SolarPanelState("solar-1", efficiency = 0.2), Surplus(1.kwh))
      val elapsedMs = (System.nanoTime() - start) / 1000000L

      elapsedMs should be >= (maxBlockMs - 300).toLong
    finally
      coldProducer.close(java.time.Duration.ofMillis(100))
  }

}
