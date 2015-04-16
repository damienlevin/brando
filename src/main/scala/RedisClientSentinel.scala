package brando

import akka.actor._
import akka.pattern._
import akka.util._

import scala.concurrent._
import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory
import java.util.concurrent.TimeUnit

object RedisClientSentinel {
  private[brando] case object SentinelConnect
}

class RedisClientSentinel(
  master: String,
  sentinelClient: ActorRef,
  database: Int,
  auth: Option[String],
  listeners: Set[ActorRef],
  connectionTimeout: FiniteDuration,
  connectionRetryDelay: FiniteDuration,
  connectionHeartbeatDelay: Option[FiniteDuration]) extends RedisConnectionSupervisor(database, auth,
  listeners, connectionTimeout, connectionHeartbeatDelay) {

  import RedisClientSentinel._
  import ConnectionSupervisor.{ Connect, Reconnect }
  import context.dispatcher

  override def preStart: Unit = {
    listeners.map(context.watch(_))
    self ! SentinelConnect
  }

  override def disconnected: Receive =
    disconnectedWithSentinel orElse super.disconnected

  def disconnectedWithSentinel: Receive = {
    case Reconnect ⇒
      context.system.scheduler.scheduleOnce(connectionRetryDelay, self, SentinelConnect)

    case SentinelConnect ⇒
      (sentinelClient ? Request("SENTINEL", "MASTER", master)) map {
        case Response.AsStrings(res) ⇒
          val (ip, port) = extractIpPort(res)
          self ! Connect(ip, port)
      } recover { case _ ⇒ self ! Reconnect }

    case x: Connection.Connected ⇒
      isValidMaster map {
        case true ⇒
          authenticate(x)
        case false ⇒
          self ! Reconnect
      } recover { case _ ⇒ self ! Reconnect }
  }

  def extractIpPort(config: Seq[String]): (String, Int) = {
    var i, port = 0
    var ip: String = ""
    while ((i < config.size) && (ip == "" || port == 0)) {
      if (config(i) == "port") port = config(i + 1).toInt
      if (config(i) == "ip") ip = config(i + 1)
      i = i + 1
    }
    (ip, port)
  }

  def isValidMaster: Future[Boolean] = {
    (connection ? Request("INFO")) map {
      case Response.AsString(res) ⇒
        res.contains("role:master")
    }
  }
}
