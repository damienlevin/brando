package brando

import akka.actor._

import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory
import java.util.concurrent.TimeUnit

object Brando {
  def apply(): Props = apply("localhost", 6379)
  def apply(
    host: String,
    port: Int,
    database: Int = 0,
    auth: Option[String] = None,
    listeners: Set[ActorRef] = Set(),
    connectionTimeout: Option[FiniteDuration] = None,
    connectionRetryDelay: Option[FiniteDuration] = None,
    connectionRetryAttempts: Option[Int] = None,
    connectionHeartbeatDelay: Option[FiniteDuration] = None): Props = {

    val config = ConfigFactory.load()
    Props(classOf[Brando],
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

  case class AuthenticationFailed(host: String, port: Int) extends Connection.StateChange
}

class Brando(
  host: String,
  port: Int,
  database: Int,
  auth: Option[String],
  listeners: Set[ActorRef],
  connectionTimeout: FiniteDuration,
  connectionRetryDelay: Option[FiniteDuration],
  connectionRetryAttempts: Option[Int],
  connectionHeartbeatDelay: Option[FiniteDuration]) extends ConnectionSupervisor(
  database, auth, listeners, connectionTimeout, connectionRetryDelay,
  connectionRetryAttempts, connectionHeartbeatDelay) {

  import ConnectionSupervisor.Connect

  override def preStart: Unit = {
    listeners.map(context.watch(_))
    self ! Connect(host, port)
  }
}
