package brando

import akka.actor._
import akka.pattern._
import akka.util._
import scala.concurrent.duration._
import scala.concurrent.Future
import scala.util.Try

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

  private[brando] case class Connect(host: String, port: Int)
  private[brando] case object Reconnect
  case class AuthenticationFailed(host: String, port: Int) extends Connection.StateChange
}

class Brando(
    host: String,
    port: Integer,
    database: Int,
    auth: Option[String],
    var listeners: Set[ActorRef],
    connectionTimeout: FiniteDuration,
    connectionRetryDelay: Option[FiniteDuration],
    connectionRetryAttempts: Option[Int],
    connectionHeartbeatDelay: Option[FiniteDuration]) extends Actor with Stash {

  import Brando._
  import context.dispatcher

  implicit val timeout = Timeout(connectionTimeout)

  var connection = context.system.deadLetters
  var retries = 0

  override def preStart: Unit = {
    listeners.map(context.watch(_))
    self ! Connect(host, port)
  }

  def receive = disconnected

  def connected: Receive = common orElse {
    case request: Request ⇒
      connection forward request

    case batch: Batch ⇒
      connection forward batch

    case x: Connection.Disconnected ⇒
      notifyStateChange(x)
      context.become(disconnected)
      self ! Reconnect
  }

  def disconnected: Receive = common orElse {
    case request: Request ⇒
      stash()

    case batch: Batch ⇒
      stash()

    case Connect(h, p) ⇒
      connection ! PoisonPill
      connection = context.actorOf(Props(classOf[Connection],
        self, h, p, connectionTimeout, connectionHeartbeatDelay))

    case Reconnect ⇒
      retries += 1
      reconnect

    case x: Connection.Connecting ⇒
      notifyStateChange(x)

    case x: Connection.Connected ⇒
      authenticate(x)

    case ("auth_ok", x: Connection.Connected) ⇒
      retries = 0
      notifyStateChange(x)
      context.become(connected)
      unstashAll()

    case x: Connection.ConnectionFailed ⇒
      notifyStateChange(x)
      self ! Reconnect
  }

  def common: Receive = {
    case s: ActorRef ⇒
      listeners = listeners + s

    case Terminated(l) ⇒
      listeners = listeners - l
  }

  def reconnect {
    (connectionRetryDelay, connectionRetryAttempts) match {
      case (Some(delay), Some(maxAttempts)) if (maxAttempts >= retries) ⇒
        context.system.scheduler.scheduleOnce(delay, connection, Connection.Connect)
      case (Some(delay), None) ⇒
        context.system.scheduler.scheduleOnce(delay, connection, Connection.Connect)
      case _ ⇒
    }
  }

  def notifyStateChange(newState: Connection.StateChange) {
    listeners foreach { _ ! newState }
  }

  def authenticate(x: Connection.Connected) {
    (for {
      auth ← if (auth.isDefined)
        connection ? Request(ByteString("AUTH"), ByteString(auth.get)) else Future.successful(Unit)
      database ← if (database != 0)
        connection ? Request(ByteString("SELECT"), ByteString(database.toString)) else Future.successful(Unit)
    } yield ("auth_ok", x)) map {
      self ! _
    } onFailure {
      case e: Exception ⇒
        notifyStateChange(Brando.AuthenticationFailed(x.host, x.port))
    }
  }
}
