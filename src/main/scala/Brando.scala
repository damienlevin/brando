package brando

import akka.actor.{ Props ⇒ AkkaProps, _ }

import scala.concurrent.duration._
import com.typesafe.config.ConfigFactory
import java.util.concurrent.TimeUnit
import ShardManager._

object Props {
  object Sentinel {

    val config = ConfigFactory.load()

    def apply(): AkkaProps = apply(Seq(SentinelClient.Sentinel("localhost", 26379)))
    def apply(
      sentinels: Seq[SentinelClient.Sentinel],
      listeners: Set[ActorRef] = Set(),
      connectionTimeout: Option[FiniteDuration] = None,
      connectionHeartbeatDelay: Option[FiniteDuration] = None): AkkaProps = {

      AkkaProps(classOf[SentinelClient], sentinels, listeners,
        connectionTimeout.getOrElse(
          config.getDuration("brando.connection.timeout", TimeUnit.MILLISECONDS).millis),
        connectionHeartbeatDelay)

    }
  }

  object Redis {

    val config = ConfigFactory.load()

    def apply(): AkkaProps = apply("localhost", 6379)
    def apply(
      host: String,
      port: Int,
      database: Int = 0,
      auth: Option[String] = None,
      listeners: Set[ActorRef] = Set(),
      connectionTimeout: Option[FiniteDuration] = None,
      connectionRetryDelay: Option[FiniteDuration] = None,
      connectionRetryAttempts: Option[Int] = None,
      connectionHeartbeatDelay: Option[FiniteDuration] = None): AkkaProps = {

      AkkaProps(classOf[RedisClient],
        host,
        port,
        database,
        auth,
        listeners,
        connectionTimeout.getOrElse(
          config.getDuration("brando.connection.timeout", TimeUnit.MILLISECONDS).millis),
        Some(connectionRetryDelay.getOrElse(
          config.getDuration("brando.connection.retry.delay", TimeUnit.MILLISECONDS).millis)),
        connectionRetryAttempts,
        connectionHeartbeatDelay)
    }

    def withSentinel(
      master: String,
      sentinelClient: ActorRef,
      database: Int = 0,
      auth: Option[String] = None,
      listeners: Set[ActorRef] = Set(),
      connectionTimeout: Option[FiniteDuration] = None,
      connectionRetryDelay: Option[FiniteDuration] = None,
      connectionHeartbeatDelay: Option[FiniteDuration] = None): AkkaProps = {

      AkkaProps(classOf[RedisClientSentinel],
        master,
        sentinelClient,
        database,
        auth,
        listeners,
        connectionTimeout.getOrElse(
          config.getDuration("brando.connection.timeout", TimeUnit.MILLISECONDS).millis),
        connectionRetryDelay.getOrElse(
          config.getDuration("brando.connection.retry.delay", TimeUnit.MILLISECONDS).millis),
        connectionHeartbeatDelay)
    }

    def withShards(
      shards: Seq[Shard],
      listeners: Set[ActorRef] = Set(),
      hashFunction: (Array[Byte] ⇒ Long) = ShardManager.defaultHashFunction,
      connectionTimeout: Option[FiniteDuration] = None,
      connectionRetryDelay: Option[FiniteDuration] = None,
      connectionHeartbeatDelay: Option[FiniteDuration] = None): AkkaProps = {

      AkkaProps(classOf[ShardManager], shards, hashFunction, listeners, None,
        connectionTimeout.getOrElse(
          config.getDuration("brando.connection.timeout", TimeUnit.MILLISECONDS).millis),
        connectionRetryDelay.getOrElse(
          config.getDuration("brando.connection.retry.delay", TimeUnit.MILLISECONDS).millis),
        connectionHeartbeatDelay)
    }

    def withShardsAndSentinel(
      shards: Seq[Shard],
      listeners: Set[ActorRef] = Set(),
      sentinelClient: ActorRef,
      hashFunction: (Array[Byte] ⇒ Long) = ShardManager.defaultHashFunction,
      connectionTimeout: Option[FiniteDuration] = None,
      connectionRetryDelay: Option[FiniteDuration] = None,
      connectionHeartbeatDelay: Option[FiniteDuration] = None): AkkaProps = {

      AkkaProps(classOf[ShardManager], shards, hashFunction, listeners, Some(sentinelClient),
        connectionTimeout.getOrElse(
          config.getDuration("brando.connection.timeout", TimeUnit.MILLISECONDS).millis),
        connectionRetryDelay.getOrElse(
          config.getDuration("brando.connection.retry.delay", TimeUnit.MILLISECONDS).millis),
        connectionHeartbeatDelay)
    }
  }
}
