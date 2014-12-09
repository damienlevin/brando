package brando

import akka.util._

import akka.actor._
import akka.pattern._
import scala.concurrent._
import scala.concurrent.duration._

object TestApp extends App with ReplyParser {

  def buildResponse(size: Int) = {
    (1 to size).foldLeft(ByteString(s"*$size\r\n")) {
      (res, el) ⇒ res ++ ByteString("$") ++ ByteString(s"${el.toString().length() + 2}\r\n") ++ ByteString(s"el$el\r\n")
    }
  }

  def testParsing(list: Int*) {
    list map { size ⇒
      val resp = buildResponse(size)
      val start = System.currentTimeMillis
      parseReply(resp) { _ ⇒
        println(s"Done pasing list of $size in ${System.currentTimeMillis - start} ms")
      }
    }
  }

  println("Starting parsing perf test...")
  try {
    testParsing(100, 1000, 2000, 4000, 8000, 16000, 30000)
  } catch {
    case e: Exception ⇒ e.printStackTrace

  }

  /*println("Starting fetching redis data from db...")

  implicit val t = Timeout(300.seconds)
  import scala.concurrent.ExecutionContext.Implicits.global

  val system = ActorSystem("system")
  val redis = system.actorOf(Brando("rds-dur1.ntg.mdialog.com", 6383))
  val start = System.currentTimeMillis
  //redis ? Request("SMEMBERS", "assets:failed") map {
  redis ? Request("SMEMBERS", "assets:completed") map {
    case x ⇒
      println(s"Got response in ${System.currentTimeMillis - start} ms")
  } recover {
    case e: Exception ⇒
      println(s"Error $e")
  }*/

}
