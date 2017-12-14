package Actors

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.testkit.{TestActorRef, TestKit}
import org.scalatest.{BeforeAndAfterAll, MustMatchers, Suite, WordSpecLike}

object HttpActorProtocol {
  case class SendRequest(data: String)
  case class GetState(receiver: ActorRef)
}

class TestOutActor extends Actor{
  import HttpActorProtocol._

  var internalState = Vector[String]()
  override def receive = {
    case SendRequest(data) =>
      internalState = internalState :+ data
      sender ! "received"
    case GetState(receiver) => receiver ! internalState
  }
  def state = internalState
}

// Stop the system after all tests are done
trait StopSystemAfterAll extends BeforeAndAfterAll {
  this: TestKit with Suite =>

  override protected def afterAll() {
    super.afterAll()
    system.terminate()
  }
}

class HttpActorSpec extends TestKit(ActorSystem("testsystem"))
  with WordSpecLike         // WordSpec provides an easy to read DSL for testing in the BDD style
  with MustMatchers     // MustMatchers provides easy to read assertions
  with StopSystemAfterAll {

  "A Http Actor" must {
    import HttpActorProtocol._

    "change state when it receives a message two, single threaded" in {
      val httpActorMock = TestActorRef[TestOutActor]
      //val httpTestActor =system.actorOf(Props[HttpActor])
      httpActorMock ! SendRequest("two")
      // Get the underlying actor and assert the state
      //expectMsg("received")
      httpActorMock.underlyingActor.state must (contain("two"))
    }
  }

  "A Http Actor when sent only src" must {
    import HttpActorProtocol._

    "change state when it receives a message one, single threaded" in {
      val httpActorMockOne = TestActorRef[TestOutActor]
      //val httpTestActor =system.actorOf(Props[HttpActor])
      httpActorMockOne ! SendRequest("one")
      // Get the underlying actor and assert the state
      //expectMsg("received")
      httpActorMockOne.underlyingActor.state must (contain("one"))
    }
  }

  "A Http Actor when sent aggregate message" must {
    import HttpActorProtocol._

    "change state when it receives a message aggregate, single threaded" in {
      val httpActorMock = TestActorRef[TestOutActor]
      //val httpTestActor =system.actorOf(Props[HttpActor])
      httpActorMock ! SendRequest("aggregate")
      // Get the underlying actor and assert the state
      //expectMsg("received")
      httpActorMock.underlyingActor.state must (contain("aggregate"))
    }
  }

  "A Http Actor should change state when it receives a message, multithreaded" must {
    import HttpActorProtocol._

    "change state when it receives a message aggregate, single threaded" in {
      val httpActorMock = system.actorOf(Props[TestOutActor])
      httpActorMock ! SendRequest("BOS")
      httpActorMock ! SendRequest("NYC")
      httpActorMock ! GetState(testActor)
      // Used to check what message(s) have been sent to the testActor
      expectMsg(Vector("BOS", "NYC"))
    }
  }


}
