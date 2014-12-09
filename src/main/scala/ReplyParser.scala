package brando

import annotation.tailrec
import akka.actor.Status
import akka.util.ByteString
import scala.collection.mutable.ListBuffer

sealed abstract class StatusReply(val status: String) {
  val bytes = ByteString(status)
}

object ValueType {
  case object String extends StatusReply("string")
  case object List extends StatusReply("list")
  case object Set extends StatusReply("set")
  case object ZSet extends StatusReply("set")
  case object Hash extends StatusReply("hash")
}

case object Ok extends StatusReply("OK")
case object Pong extends StatusReply("PONG")
case object Queued extends StatusReply("QUEUED")

private[brando] object StatusReply {
  import ValueType._

  def fromString(status: String) = {
    status match {
      case Ok.status     ⇒ Some(Ok)
      case Pong.status   ⇒ Some(Pong)
      case Queued.status ⇒ Some(Queued)

      case String.status ⇒ Some(String)
      case List.status   ⇒ Some(List)
      case Set.status    ⇒ Some(Set)
      case ZSet.status   ⇒ Some(ZSet)
      case Hash.status   ⇒ Some(Hash)
    }
  }
}

private[brando] trait ReplyParser {

  var remainingBuffer = ListBuffer[Byte]()

  trait Result {
    val reply: Option[Any]
    val leftBytes: Int
  }
  case class Success(reply: Option[Any], leftBytes: Int = 0) extends Result
  case class Failure(leftBytes: Int = 0) extends Result {
    val reply = None
  }

  def splitLine(buffer: ListBuffer[Byte]): Option[(String, Int)] = {
    val start = buffer.takeWhile(_ != '\r')
    if (buffer.slice(start.size, start.size + 2) == ListBuffer[Byte]('\r', '\n')) {
      Some((new String(start.drop(1).toArray), buffer.size - (start.size + 2)))
    } else {
      None
    }
  }

  def readErrorReply(buffer: ListBuffer[Byte]) = splitLine(buffer) match {
    case Some((error, leftBytes)) ⇒
      Success(Some(Status.Failure(new BrandoException(error))), leftBytes)
    case _ ⇒ Failure(buffer.size)
  }

  def readStatusReply(buffer: ListBuffer[Byte]) = splitLine(buffer) match {
    case Some((status, leftBytes)) ⇒
      Success(StatusReply.fromString(status), leftBytes)
    case _ ⇒ Failure(buffer.size)
  }

  def readIntegerReply(buffer: ListBuffer[Byte]) = splitLine(buffer) match {
    case Some((int, leftBytes)) ⇒ Success(Some(int.toLong), leftBytes)
    case x                      ⇒ Failure(buffer.size)
  }

  def readBulkReply(buffer: ListBuffer[Byte]): Result = splitLine(buffer) match {
    case Some((length, leftBytes)) ⇒
      val dataLength = length.toInt
      if (dataLength == -1) Success(None, leftBytes) //null response
      else if (leftBytes >= dataLength + 2) { //leftBytes = data + "\r\n"
        val header = buffer.size - leftBytes
        val data = buffer.slice(header, header + dataLength)
        Success(Some(data), leftBytes - (data.size + 2))
      } else Failure(buffer.size)

    case _ ⇒ Failure(buffer.size)
  }

  def readMultiBulkReply(buffer: ListBuffer[Byte]): Result = splitLine(buffer) match {

    case Some((count, leftBytes)) ⇒
      val itemCount = count.toInt

      @tailrec def readComponents(
        remaining: Int,
        result: Result,
        start: Int): Result = remaining match {
        case 0                           ⇒ result
        case _ if (buffer.size == start) ⇒ Failure(buffer.size)
        case i ⇒
          parse(buffer.slice(start, buffer.size)) match {
            case failure: Failure ⇒ Failure(buffer.size)
            case Success(newReply, left) ⇒
              val replyList = result.reply.map(_.asInstanceOf[ListBuffer[Option[Any]]])
              val newReplyList = replyList map (_ :+ newReply)
              readComponents(i - 1, Success(newReplyList, left), buffer.size - left)
          }
      }

      readComponents(itemCount, Success(Some(ListBuffer.empty[Option[Any]])), buffer.size - leftBytes)

    case _ ⇒ Failure(buffer.size)
  }

  def readPubSubMessage(buffer: ListBuffer[Byte]) = splitLine(buffer) match {
    case Some((int, leftBytes)) ⇒ Success(Some(int.toLong), leftBytes)
    case x                      ⇒ Failure(buffer.size)
  }

  def parse(bytes: ByteString): Result = {
    parse(ListBuffer[Byte](bytes: _*))
  }

  final def parseReply(bytes: ByteString)(withReply: Any ⇒ Unit) {
    parseReply(ListBuffer[Byte](bytes: _*))(withReply)
  }

  def parse(reply: ListBuffer[Byte]) = reply(0) match {
    case '+' ⇒ readStatusReply(reply)
    case ':' ⇒ readIntegerReply(reply)
    case '$' ⇒ readBulkReply(reply)
    case '*' ⇒ readMultiBulkReply(reply)
    case '-' ⇒ readErrorReply(reply)
  }

  @tailrec final def parseReply(bytes: ListBuffer[Byte])(withReply: Any ⇒ Unit) {
    if (bytes.size > 0) remainingBuffer.appendAll(bytes)
    if (remainingBuffer.size > 0) {
      parse(remainingBuffer) match {
        case Failure(leftBytes) ⇒
        case Success(reply, leftBytes) ⇒
          remainingBuffer = remainingBuffer.drop(remainingBuffer.size - leftBytes)
          withReply(reply)
          parseReply(ListBuffer[Byte]())(withReply)
      }
    } else {
      Failure(bytes.size)
    }
  }
}
