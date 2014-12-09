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
    val next: ListBuffer[Byte]
  }
  case class Success(reply: Option[Any], next: ListBuffer[Byte] = ListBuffer[Byte]())
    extends Result
  case class Failure(next: ListBuffer[Byte])
      extends Result {
    val reply = None
  }

  def splitLine(buffer: ListBuffer[Byte]): Option[(String, ListBuffer[Byte])] = {
    val start = buffer.takeWhile(_ != '\r')
    val rest = buffer.drop(start.size)
    if (rest.take(2) == ListBuffer[Byte]('\r', '\n')) {
      Some((new String(start.drop(1).toArray), rest.drop(2)))
    } else {
      None
    }
  }

  def readErrorReply(buffer: ListBuffer[Byte]) = splitLine(buffer) match {
    case Some((error, rest)) ⇒
      Success(Some(Status.Failure(new BrandoException(error))), rest)
    case _ ⇒ Failure(buffer)
  }

  def readStatusReply(buffer: ListBuffer[Byte]) = splitLine(buffer) match {
    case Some((status, rest)) ⇒
      Success(StatusReply.fromString(status), rest)
    case _ ⇒ Failure(buffer)
  }

  def readIntegerReply(buffer: ListBuffer[Byte]) = splitLine(buffer) match {
    case Some((int, rest)) ⇒ Success(Some(int.toLong), rest)
    case x                 ⇒ Failure(buffer)
  }

  def readBulkReply(buffer: ListBuffer[Byte]): Result = splitLine(buffer) match {
    case Some((length, rest)) ⇒
      val dataLength = length.toInt

      if (dataLength == -1) Success(None, rest) //null response
      else if (rest.length >= dataLength + 2) { //rest = data + "\r\n"
        val data = rest.take(dataLength)
        val remainder = rest.drop(dataLength + 2)
        Success(Some(data), remainder)
      } else Failure(buffer)

    case _ ⇒ Failure(buffer)
  }

  def readMultiBulkReply(buffer: ListBuffer[Byte]): Result = splitLine(buffer) match {

    case Some((count, rest)) ⇒
      val itemCount = count.toInt

      @tailrec def readComponents(remaining: Int, result: Result): Result = remaining match {
        case 0                        ⇒ result
        case _ if result.next.isEmpty ⇒ Failure(buffer)
        case i ⇒
          parse(result.next) match {
            case failure: Failure ⇒ Failure(buffer)

            case Success(newReply, next) ⇒
              val replyList =
                result.reply.map(_.asInstanceOf[ListBuffer[Option[Any]]])
              val newReplyList = replyList map (_ :+ newReply)

              readComponents(i - 1, Success(newReplyList, next))
          }
      }

      readComponents(itemCount, Success(Some(ListBuffer.empty[Option[Any]]), rest))

    case _ ⇒ Failure(buffer)
  }

  def readPubSubMessage(buffer: ListBuffer[Byte]) = splitLine(buffer) match {
    case Some((int, rest)) ⇒ Success(Some(int.toLong), rest)
    case x                 ⇒ Failure(buffer)
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
    if (bytes.size > 0) {
      parse(remainingBuffer ++ bytes) match {
        case Failure(leftoverBytes) ⇒
          remainingBuffer = leftoverBytes

        case Success(reply, leftoverBytes) ⇒
          remainingBuffer = ListBuffer[Byte]()
          withReply(reply)

          if (leftoverBytes.size > 0) {
            parseReply(leftoverBytes)(withReply)
          }
      }
    } else Failure(bytes)
  }
}
