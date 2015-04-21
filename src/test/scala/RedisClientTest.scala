package brando

import org.scalatest.FunSpecLike
import akka.testkit._
import akka.actor._
import akka.pattern._
import akka.io.{ IO, Tcp }
import akka.util._
import scala.concurrent.Await
import scala.concurrent.duration._
import java.util.UUID
import java.net.ServerSocket

class RedisClientTest extends TestKit(ActorSystem("RedisClientTest")) with FunSpecLike
    with ImplicitSender {

  import Connection._

  describe("ping") {
    it("should respond with Pong") {
      val brando = system.actorOf(Props.Redis())

      brando ! Request("PING")

      expectMsg(Some(Pong))
    }
  }

  describe("flushdb") {
    it("should respond with OK") {
      val brando = system.actorOf(Props.Redis())

      brando ! Request("FLUSHDB")

      expectMsg(Some(Ok))
    }
  }

  describe("set") {
    it("should respond with OK") {
      val brando = system.actorOf(Props.Redis())

      brando ! Request("SET", "mykey", "somevalue")

      expectMsg(Some(Ok))

      brando ! Request("FLUSHDB")
      expectMsg(Some(Ok))
    }
  }

  describe("get") {
    it("should respond with value option for existing key") {
      val brando = system.actorOf(Props.Redis())

      brando ! Request("SET", "mykey", "somevalue")

      expectMsg(Some(Ok))

      brando ! Request("GET", "mykey")

      expectMsg(Some(ByteString("somevalue")))

      brando ! Request("FLUSHDB")
      expectMsg(Some(Ok))
    }

    it("should respond with None for non-existent key") {
      val brando = system.actorOf(Props.Redis())

      brando ! Request("GET", "mykey")

      expectMsg(None)
    }
  }

  describe("incr") {
    it("should increment and return value for existing key") {
      val brando = system.actorOf(Props.Redis())

      brando ! Request("SET", "incr-test", "10")

      expectMsg(Some(Ok))

      brando ! Request("INCR", "incr-test")

      expectMsg(Some(11))

      brando ! Request("FLUSHDB")
      expectMsg(Some(Ok))
    }

    it("should return 1 for non-existent key") {
      val brando = system.actorOf(Props.Redis())

      brando ! Request("INCR", "incr-test")

      expectMsg(Some(1))

      brando ! Request("FLUSHDB")
      expectMsg(Some(Ok))
    }
  }

  describe("sadd") {
    it("should return number of members added to set") {
      val brando = system.actorOf(Props.Redis())

      brando ! Request("SADD", "sadd-test", "one")

      expectMsg(Some(1))

      brando ! Request("SADD", "sadd-test", "two", "three")

      expectMsg(Some(2))

      brando ! Request("SADD", "sadd-test", "one", "four")

      expectMsg(Some(1))

      brando ! Request("FLUSHDB")
      expectMsg(Some(Ok))
    }
  }

  describe("smembers") {
    it("should return all members in a set") {
      val brando = system.actorOf(Props.Redis())

      brando ! Request("SADD", "smembers-test", "one", "two", "three", "four")

      expectMsg(Some(4))

      brando ! Request("SMEMBERS", "smembers-test")

      val resp = receiveOne(500.millis).asInstanceOf[Option[List[Any]]]
      assert(resp.getOrElse(List()).toSet ===
        Set(Some(ByteString("one")), Some(ByteString("two")),
          Some(ByteString("three")), Some(ByteString("four"))))

      brando ! Request("FLUSHDB")
      expectMsg(Some(Ok))
    }

  }

  describe("pipelining") {
    it("should respond to a Seq of multiple requests all at once") {
      val brando = system.actorOf(Props.Redis())
      val ping = Request("PING")

      brando ! ping
      brando ! ping
      brando ! ping

      expectMsg(Some(Pong))
      expectMsg(Some(Pong))
      expectMsg(Some(Pong))

    }

    it("should support pipelines of setex commands") {
      val brando = system.actorOf(Props.Redis())
      val setex = Request("SETEX", "pipeline-setex-path", "10", "Some data")

      brando ! setex
      brando ! setex
      brando ! setex

      expectMsg(Some(Ok))
      expectMsg(Some(Ok))
      expectMsg(Some(Ok))
    }

    it("should receive responses in the right order") {
      val brando = system.actorOf(Props.Redis())
      val ping = Request("PING")
      val setex = Request("SETEX", "pipeline-setex-path", "10", "Some data")

      brando ! setex
      brando ! ping
      brando ! setex
      brando ! ping
      brando ! setex

      expectMsg(Some(Ok))
      expectMsg(Some(Pong))
      expectMsg(Some(Ok))
      expectMsg(Some(Pong))
      expectMsg(Some(Ok))
    }
  }

  describe("large data sets") {
    it("should read and write large files") {
      import java.io.{ File, FileInputStream }

      val file = new File("src/test/resources/crime_and_punishment.txt")
      val in = new FileInputStream(file)
      val bytes = new Array[Byte](file.length.toInt)
      in.read(bytes)
      in.close()

      val largeText = new String(bytes, "UTF-8")

      val brando = system.actorOf(Props.Redis())

      brando ! Request("SET", "crime+and+punishment", largeText)

      expectMsg(Some(Ok))

      brando ! Request("GET", "crime+and+punishment")

      expectMsg(Some(ByteString(largeText)))

      brando ! Request("FLUSHDB")
      expectMsg(Some(Ok))
    }
  }

  describe("error reply") {
    it("should receive a failure with the redis error message") {
      val brando = system.actorOf(Props.Redis())

      brando ! Request("SET", "key")

      expectMsgPF(5.seconds) {
        case Status.Failure(e) ⇒
          assert(e.isInstanceOf[BrandoException])
          assert(e.getMessage === "ERR wrong number of arguments for 'set' command")
      }

      brando ! Request("EXPIRE", "1", "key")

      expectMsgPF(5.seconds) {
        case Status.Failure(e) ⇒
          assert(e.isInstanceOf[BrandoException])
          assert(e.getMessage === "ERR value is not an integer or out of range")
      }
    }
  }

  describe("select") {
    it("should execute commands on the selected database") {
      val brando = system.actorOf(Props.Redis("localhost", 6379, 5))

      brando ! Request("SET", "mykey", "somevalue")

      expectMsg(Some(Ok))

      brando ! Request("GET", "mykey")

      expectMsg(Some(ByteString("somevalue")))

      brando ! Request("SELECT", "0")

      expectMsg(Some(Ok))

      brando ! Request("GET", "mykey")

      expectMsg(None)

      brando ! Request("SELECT", "5")
      expectMsg(Some(Ok))

      brando ! Request("FLUSHDB")
      expectMsg(Some(Ok))
    }
  }

  describe("multi/exec requests") {
    it("should support multi requests as an atomic transaction") {
      val brando = system.actorOf(Props.Redis("localhost", 6379, 5))
      brando ! Batch(Request("MULTI"), Request("SET", "mykey", "somevalue"), Request("GET", "mykey"), Request("EXEC"))
      expectMsg(List(Some(Ok),
        Some(Queued),
        Some(Queued),
        Some(List(Some(Ok), Some(ByteString("somevalue"))))))
    }

    it("should support multi requests with multiple results") {
      val brando = system.actorOf(Props.Redis("localhost", 6379, 5))
      brando ! Batch(Request("MULTI"), Request("SET", "mykey", "somevalue"), Request("GET", "mykey"), Request("GET", "mykey"), Request("EXEC"))
      expectMsg(List(Some(Ok),
        Some(Queued),
        Some(Queued),
        Some(Queued),
        Some(List(Some(Ok), Some(ByteString("somevalue")), Some(ByteString("somevalue"))))))
    }
  }

  describe("blocking requests") {
    describe("subscribe") {

      it("should be able to subscribe to a pubsub channel") {
        val channel = UUID.randomUUID().toString
        val subscriber = system.actorOf(Props.Redis())

        subscriber ! Request("SUBSCRIBE", channel)

        expectMsg(Some(List(Some(
          ByteString("subscribe")),
          Some(ByteString(channel)),
          Some(1))))
      }

      it("should receive published messages from a pubsub channel") {
        val channel = UUID.randomUUID().toString
        val subscriber = system.actorOf(Props.Redis())
        val publisher = system.actorOf(Props.Redis())

        subscriber ! Request("SUBSCRIBE", channel)

        expectMsg(Some(List(Some(
          ByteString("subscribe")),
          Some(ByteString(channel)),
          Some(1))))

        publisher ! Request("PUBLISH", channel, "test")
        expectMsg(Some(1)) //publisher gets back number of subscribers when publishing

        expectMsg(PubSubMessage(channel, "test"))
      }

      it("should be able to unsubscribe from a pubsub channel") {
        val channel = UUID.randomUUID().toString
        val subscriber = system.actorOf(Props.Redis())
        val publisher = system.actorOf(Props.Redis())

        subscriber ! Request("SUBSCRIBE", channel)

        expectMsg(Some(List(Some(
          ByteString("subscribe")),
          Some(ByteString(channel)),
          Some(1))))

        subscriber ! Request("UNSUBSCRIBE", channel)

        expectMsg(Some(List(Some(
          ByteString("unsubscribe")),
          Some(ByteString(channel)),
          Some(0))))

        publisher ! Request("PUBLISH", channel, "test")
        expectMsg(Some(0))

        expectNoMsg
      }
    }

    describe("should be able to block on blpop") {
      val brando = system.actorOf(Props.Redis())
      try {
        val channel = UUID.randomUUID().toString
        val popRedisClient = system.actorOf(Props.Redis())

        val probeRedisClient = TestProbe()
        val probePopRedisClient = TestProbe()

        popRedisClient.tell(Request("BLPOP", "blpop:list", "0"), probePopRedisClient.ref)

        probePopRedisClient.expectNoMsg

        brando.tell(Request("LPUSH", "blpop:list", "blpop-value"), probeRedisClient.ref)

        probePopRedisClient.expectMsg(
          Some(List(Some(
            ByteString("blpop:list")),
            Some(ByteString("blpop-value")))))

        probeRedisClient.expectMsg(Some(1))

      } finally {
        implicit val timeout = Timeout(1.seconds)
        Await.ready((brando ? Request("del", "blpop:list")), 1.seconds)
      }
    }
  }

  describe("notifications") {
    it("should send a Connected event if connecting succeeds") {
      val probe = TestProbe()
      val brando = system.actorOf(Props.Redis("localhost", 6379, listeners = Set(probe.ref)))

      probe.expectMsg(Connecting("localhost", 6379))
      probe.expectMsg(Connected("localhost", 6379))
    }

    it("should send an ConnectionFailed event if connecting fails") {
      val probe = TestProbe()
      val brando = system.actorOf(Props.Redis("localhost", 13579, listeners = Set(probe.ref)))

      probe.expectMsg(Connecting("localhost", 13579))
      probe.expectMsg(ConnectionFailed("localhost", 13579))
    }

    it("should send an AuthenticationFailed event if connecting succeeds but authentication fails") {
      val probe = TestProbe()
      val brando = system.actorOf(Props.Redis("localhost", 6379, auth = Some("not-the-auth"), listeners = Set(probe.ref)))

      probe.expectMsg(Connecting("localhost", 6379))
      probe.expectMsg(RedisClient.AuthenticationFailed("localhost", 6379))
    }

    it("should send a ConnectionFailed if redis is not responsive during connection") {
      val serverSocket = new ServerSocket(0)
      val port = serverSocket.getLocalPort()

      val probe = TestProbe()
      val brando = system.actorOf(Props.Redis("localhost", port, listeners = Set(probe.ref)))

      probe.expectMsg(Connecting("localhost", port))
      probe.expectMsg(ConnectionFailed("localhost", port))
    }

    it("should send a notification to later added listener") {
      val probe = TestProbe()
      val probe2 = TestProbe()
      val brando = system.actorOf(Props.Redis("localhost", 13579, listeners = Set(probe2.ref)))
      brando ! probe.ref

      probe2.expectMsg(Connecting("localhost", 13579))
      probe.expectMsg(Connecting("localhost", 13579))
      probe2.expectMsg(ConnectionFailed("localhost", 13579))
      probe.expectMsg(ConnectionFailed("localhost", 13579))
    }
  }

  describe("connection") {
    import Connection._
    it("should try to reconnect if connectionRetryDelay and connectionRetryAttempts are defined") {
      val listener = TestProbe()
      val brando = TestActorRef(new RedisClient(
        "localhost", 6379, 0, None, Set(listener.ref), 2.seconds, Some(1.seconds), Some(1), None))

      listener.expectMsg(Connecting("localhost", 6379))
      assert(brando.underlyingActor.retries === 0)
      listener.expectMsg(Connected("localhost", 6379))
      assert(brando.underlyingActor.retries === 0)

      brando ! Disconnected("localhost", 6379)

      listener.expectMsg(Disconnected("localhost", 6379))
      listener.expectMsg(Connecting("localhost", 6379))
    }

    it("should not try to reconnect if connectionRetryDelay and connectionRetryAttempts are not defined") {
      val listener = TestProbe()
      val brando = TestActorRef(new RedisClient(
        "localhost", 6379, 0, None, Set(listener.ref), 2.seconds, None, None, None))

      listener.expectMsg(Connecting("localhost", 6379))
      listener.expectMsg(Connected("localhost", 6379))

      brando ! Disconnected("localhost", 6379)

      listener.expectMsg(Disconnected("localhost", 6379))
      listener.expectNoMsg
    }

    it("should not try to reconnect once the max retry attempts is reached") {
      val listener = TestProbe()
      val brando = TestActorRef(new RedisClient(
        "localhost", 16379, 0, None, Set(listener.ref), 2.seconds, Some(1.seconds), Some(1), None))

      listener.expectMsg(Connecting("localhost", 16379))
      assert(brando.underlyingActor.retries === 0)
      listener.expectMsg(ConnectionFailed("localhost", 16379))

      listener.expectMsg(Connecting("localhost", 16379))
      assert(brando.underlyingActor.retries === 1)
      listener.expectMsg(ConnectionFailed("localhost", 16379))

      listener.expectNoMsg
    }
  }
}
