package Actors

import akka.actor.{ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestKit}
import org.scalatest._


class ApiActorSpec
  extends TestKit(ActorSystem("ApiActorSpec"))
    with ImplicitSender
    with WordSpecLike
    with BeforeAndAfterAll
    with Matchers {
  //import context.dispatcher


  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  val apiActor = system.actorOf(Props[APIActor])
  "An APIActor using TestActorRef " must {
    "Make an aggregated request" in {
      apiActor ! AggregatedUserRequest("bos","nyc",apiActor)

    }
  }

  "An APIActor using TestActorRef 2" must {
    "Make an Inpsired request" in {
      apiActor ! InpsiredUserRequest("bos",Kafka.AmadeusProducer.TOPIC,apiActor)

    }
  }

  "An APIActor using TestActorRef 3 " must {
    "Make an normal request" in {
      apiActor ! UserRequest("bos","nyc",Kafka.AmadeusProducer.TOPIC,apiActor)

    }
  }
}