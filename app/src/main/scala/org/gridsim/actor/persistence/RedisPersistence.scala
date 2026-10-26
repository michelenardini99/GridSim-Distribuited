package org.gridsim.actor.persistence

import com.typesafe.config.Config
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.persistence.{AtomicWrite, PersistentRepr, SelectedSnapshot, SnapshotMetadata, SnapshotSelectionCriteria}
import org.apache.pekko.persistence.journal.AsyncWriteJournal
import org.apache.pekko.persistence.snapshot.SnapshotStore
import org.apache.pekko.serialization.{Serialization, SerializationExtension}
import redis.clients.jedis.{JedisPool, JedisPoolConfig}

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.nio.charset.StandardCharsets
import java.util.Base64
import scala.collection.immutable.Seq
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters.*
import scala.util.{Try, Using}

object RedisClientPool:
  @volatile private var poolOpt: Option[(String, Int, JedisPool)] = None

  def getPool(host: String, port: Int): JedisPool = synchronized {
    poolOpt match
      case Some((h, p, pool)) if h == host && p == port && !pool.isClosed =>
        pool
      case _ =>
        poolOpt.foreach(_._3.close())
        val poolConfig = new JedisPoolConfig()
        poolConfig.setMaxTotal(32)
        poolConfig.setMaxIdle(16)
        val pool = new JedisPool(poolConfig, host, port)
        poolOpt = Some((host, port, pool))
        pool
  }

  final case class SnapshotEntry(
      sequenceNr: Long,
      timestamp: Long,
      snapshotBytes: Array[Byte]
  ) extends java.io.Serializable

  def toBase64(bytes: Array[Byte]): String =
    Base64.getEncoder.encodeToString(bytes)

  def fromBase64(encoded: String): Array[Byte] =
    Base64.getDecoder.decode(encoded)

  def javaSerialize(obj: AnyRef): Array[Byte] =
    val bos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(bos)
    oos.writeObject(obj)
    oos.close()
    bos.toByteArray

  def javaDeserialize[T](bytes: Array[Byte]): T =
    val bis = new ByteArrayInputStream(bytes)
    val ois = new ObjectInputStream(bis)
    try ois.readObject().asInstanceOf[T]
    finally ois.close()

/** Shared Pekko Persistence Journal backed by Redis Sorted Sets (`ZSET`).
  *
  * For each `persistenceId`:
  *   - `gridsim:journal:events:<persistenceId>` is a Redis Sorted Set where the
  *     score is `sequenceNr` and the member is `"<sequenceNr>:<base64(PersistentRepr)>"`
  *   - `gridsim:journal:max_seq:<persistenceId>` tracks the highest sequence
  *     number ever written for that entity, ensuring monotonic sequence numbers
  *     even after event deletion on snapshot.
  */
class RedisJournal(config: Config) extends AsyncWriteJournal:

  private val host: String =
    if config.hasPath("host") then config.getString("host") else "localhost"
  private val port: Int =
    if config.hasPath("port") then config.getInt("port") else 6379

  private lazy val serialization: Serialization =
    SerializationExtension(context.system)
  private given ExecutionContext = context.dispatcher

  private def eventsKey(persistenceId: String): String =
    s"gridsim:journal:events:$persistenceId"

  private def maxSeqKey(persistenceId: String): String =
    s"gridsim:journal:max_seq:$persistenceId"

  override def asyncWriteMessages(
      messages: Seq[AtomicWrite]
  ): Future[Seq[Try[Unit]]] = Future {
    val pool = RedisClientPool.getPool(host, port)
    messages.map { atomicWrite =>
      Try {
        Using.resource(pool.getResource) { jedis =>
          val pid = atomicWrite.persistenceId
          val eKey = eventsKey(pid)
          val mKey = maxSeqKey(pid)
          val pipe = jedis.pipelined()
          var highestSeq = 0L

          atomicWrite.payload.foreach { repr =>
            val bytes = serialization.serialize(repr).get
            val encoded = s"${repr.sequenceNr}:${RedisClientPool.toBase64(bytes)}"
            pipe.zadd(eKey, repr.sequenceNr.toDouble, encoded)
            if repr.sequenceNr > highestSeq then highestSeq = repr.sequenceNr
          }
          pipe.sync()

          val currentMax = Option(jedis.get(mKey)).flatMap(_.toLongOption).getOrElse(0L)
          if highestSeq > currentMax then
            jedis.set(mKey, highestSeq.toString)
        }
      }
    }
  }

  override def asyncDeleteMessagesTo(
      persistenceId: String,
      toSequenceNr: Long
  ): Future[Unit] = Future {
    val pool = RedisClientPool.getPool(host, port)
    Using.resource(pool.getResource) { jedis =>
      jedis.zremrangeByScore(eventsKey(persistenceId), Double.NegativeInfinity, toSequenceNr.toDouble)
    }
  }

  override def asyncReplayMessages(
      persistenceId: String,
      fromSequenceNr: Long,
      toSequenceNr: Long,
      max: Long
  )(recoveryCallback: PersistentRepr => Unit): Future[Unit] = Future {
    if max > 0 && fromSequenceNr <= toSequenceNr then
      val pool = RedisClientPool.getPool(host, port)
      val members = Using.resource(pool.getResource) { jedis =>
        jedis
          .zrangeByScore(
            eventsKey(persistenceId),
            fromSequenceNr.toDouble,
            toSequenceNr.toDouble
          )
          .asScala
          .toList
      }
      val limit = if max > Int.MaxValue then Int.MaxValue else max.toInt
      members.take(limit).foreach { entry =>
        val idx = entry.indexOf(':')
        val base64Part = if idx >= 0 then entry.substring(idx + 1) else entry
        val bytes = RedisClientPool.fromBase64(base64Part)
        val repr = serialization.deserialize(bytes, classOf[PersistentRepr]).get
        recoveryCallback(repr)
      }
  }

  override def asyncReadHighestSequenceNr(
      persistenceId: String,
      fromSequenceNr: Long
  ): Future[Long] = Future {
    val pool = RedisClientPool.getPool(host, port)
    Using.resource(pool.getResource) { jedis =>
      val storedMax =
        Option(jedis.get(maxSeqKey(persistenceId))).flatMap(_.toLongOption).getOrElse(0L)
      val zsetMax = jedis
        .zrevrangeWithScores(eventsKey(persistenceId), 0, 0)
        .asScala
        .headOption
        .map(_.getScore.toLong)
        .getOrElse(0L)
      math.max(storedMax, zsetMax)
    }
  }

/** Shared Pekko Persistence SnapshotStore backed by Redis Sorted Sets (`ZSET`).
  */
class RedisSnapshotStore(config: Config) extends SnapshotStore:

  private val host: String =
    if config.hasPath("host") then config.getString("host") else "localhost"
  private val port: Int =
    if config.hasPath("port") then config.getInt("port") else 6379

  private given ExecutionContext = context.dispatcher

  private def snapshotsKey(persistenceId: String): String =
    s"gridsim:snapshots:$persistenceId"

  private def loadEntries(persistenceId: String): List[RedisClientPool.SnapshotEntry] =
    val pool = RedisClientPool.getPool(host, port)
    Using.resource(pool.getResource) { jedis =>
      jedis
        .zrevrange(snapshotsKey(persistenceId), 0, -1)
        .asScala
        .toList
        .flatMap { encoded =>
          val idx = encoded.indexOf(':')
          val base64Part = if idx >= 0 then encoded.substring(idx + 1) else encoded
          Try(
            RedisClientPool.javaDeserialize[RedisClientPool.SnapshotEntry](
              RedisClientPool.fromBase64(base64Part)
            )
          ).toOption
        }
    }

  override def loadAsync(
      persistenceId: String,
      criteria: SnapshotSelectionCriteria
  ): Future[Option[SelectedSnapshot]] = Future {
    loadEntries(persistenceId)
      .filter { e =>
        e.sequenceNr >= criteria.minSequenceNr &&
        e.sequenceNr <= criteria.maxSequenceNr &&
        e.timestamp >= criteria.minTimestamp &&
        e.timestamp <= criteria.maxTimestamp
      }
      .sortBy(e => (-e.sequenceNr, -e.timestamp))
      .headOption
      .map { entry =>
        val snapshotObj = RedisClientPool.javaDeserialize[Any](entry.snapshotBytes)
        SelectedSnapshot(
          SnapshotMetadata(persistenceId, entry.sequenceNr, entry.timestamp),
          snapshotObj
        )
      }
  }

  override def saveAsync(
      metadata: SnapshotMetadata,
      snapshot: Any
  ): Future[Unit] = Future {
    val pool = RedisClientPool.getPool(host, port)
    val snapBytes = RedisClientPool.javaSerialize(snapshot.asInstanceOf[AnyRef])
    val entry = RedisClientPool.SnapshotEntry(
      metadata.sequenceNr,
      metadata.timestamp,
      snapBytes
    )
    val encoded =
      s"${metadata.sequenceNr}:${RedisClientPool.toBase64(RedisClientPool.javaSerialize(entry))}"
    Using.resource(pool.getResource) { jedis =>
      val key = snapshotsKey(metadata.persistenceId)
      jedis.zremrangeByScore(key, metadata.sequenceNr.toDouble, metadata.sequenceNr.toDouble)
      jedis.zadd(key, metadata.sequenceNr.toDouble, encoded)
    }
  }

  override def deleteAsync(metadata: SnapshotMetadata): Future[Unit] = Future {
    val pool = RedisClientPool.getPool(host, port)
    Using.resource(pool.getResource) { jedis =>
      jedis.zremrangeByScore(
        snapshotsKey(metadata.persistenceId),
        metadata.sequenceNr.toDouble,
        metadata.sequenceNr.toDouble
      )
    }
  }

  override def deleteAsync(
      persistenceId: String,
      criteria: SnapshotSelectionCriteria
  ): Future[Unit] = Future {
    val pool = RedisClientPool.getPool(host, port)
    Using.resource(pool.getResource) { jedis =>
      val key = snapshotsKey(persistenceId)
      val rawMembers = jedis.zrange(key, 0, -1).asScala.toList
      rawMembers.foreach { encoded =>
        val idx = encoded.indexOf(':')
        val base64Part = if idx >= 0 then encoded.substring(idx + 1) else encoded
        Try(
          RedisClientPool.javaDeserialize[RedisClientPool.SnapshotEntry](
            RedisClientPool.fromBase64(base64Part)
          )
        ).foreach { e =>
          if e.sequenceNr >= criteria.minSequenceNr &&
            e.sequenceNr <= criteria.maxSequenceNr &&
            e.timestamp >= criteria.minTimestamp &&
            e.timestamp <= criteria.maxTimestamp
          then jedis.zrem(key, encoded)
        }
      }
    }
  }

