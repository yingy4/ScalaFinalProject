package Actors

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.stream.{ActorMaterializer, ActorMaterializerSettings}
import akka.util.ByteString
import play.api.libs.json.JsValue


case class UserRequest(src:String,dest:String,topic:String)

object HttpActor{
  def props(out:ActorRef) = Props(new HttpActor(out))
}


/** Handles HTTP Requests
  *
  * @param out an Actor reference obj which pipes in and out
  */
//TODO:: remove print statements, introduce logging?
class HttpActor(out:ActorRef) extends Actor with ActorLogging{

  val system = ActorSystem("httpSystem")

  val apiActor = system.actorOf(Props[APIActor])
  override def receive: Receive = {
    case msg:String =>
      val inputArray = msg.split("\\*")
      out ! ("I received your message: " + inputArray(0))
      apiActor ! UserRequest(inputArray(0),inputArray(1),Kafka.AmadeusProducer.TOPIC)
  }
}

/**
  * Child Actor of HttpActor which actually handles the API call
  */

class APIActor extends Actor{
  import context.dispatcher
  val http = Http(context.system)
  val system = ActorSystem("apiSystem")
  final implicit val materializer: ActorMaterializer = ActorMaterializer(ActorMaterializerSettings(context.system))

  override def receive: Receive = {
    case req:UserRequest =>
      println("in api actor,case obj")
      val kafkaActor = system.actorOf(Props[KafkaActor])

      // Creates HTTP Request and unwraps HttPResponse to get the response json as string
      http.singleRequest(HttpRequest(uri = s"http://localhost:9000/cheapflights/${req.src}/${req.dest}")).onComplete(response =>
        response.get.entity.dataBytes.runFold(ByteString(""))(_ ++ _).foreach {
          body => kafkaActor ! body.utf8String
        }
      )
  }

}
