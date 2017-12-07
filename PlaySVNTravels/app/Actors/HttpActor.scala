package Actors

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.stream.{ActorMaterializer, ActorMaterializerSettings}
import akka.util.ByteString


case class UserRequest(src:String,dest:String,topic:String)
case class InpsiredUserRequest(src:String, topic:String,sender:ActorRef)
case class AggregatedUserRequest(src:String,des:String,sender:ActorRef)

object HttpActor{
  def props(out:ActorRef)(src:String,des:String) = Props(new HttpActor(out)(src,des))
}


/** Handles HTTP Requests
  *
  * @param out an Actor reference obj which pipes in and out
  * @param src the flight source
  * @param des the destination of the flight
  */
//TODO:: remove print statements, introduce logging?
class HttpActor(out:ActorRef)(src:String,des:String) extends Actor with ActorLogging{

  val system = ActorSystem("httpSystem")

  val apiActor = system.actorOf(Props[APIActor])
  override def receive: Receive = {
    case "two" =>
      println("in normal case")
      println(s"src = ${src}")
      //val inputArray = msg.split("\\*")
      out ! ("I received your message: " +src)
      apiActor ! UserRequest(src,des,Kafka.AmadeusProducer.TOPIC)
    case "one" =>
      println("in tuple case")
      out ! ("I received your message: " + des)
      apiActor ! InpsiredUserRequest(src,Kafka.AmadeusProducer.TOPIC,out)
    case "aggregate" =>
      println("in aggregate case")
      apiActor ! AggregatedUserRequest(src,des,out)
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
    case req:InpsiredUserRequest=>
      val kafkaActor = system.actorOf(Props[KafkaActor])

      // Creates HTTP Request and unwraps HttPResponse to get the response json as string
      http.singleRequest(HttpRequest(uri = s"http://localhost:9000/inspireme/${req.src}")).onComplete(response =>
        response.get.entity.dataBytes.runFold(ByteString(""))(_ ++ _).foreach {
          body => req.sender ! body.utf8String
        }
      )
    case req:AggregatedUserRequest=>
      val kafkaActor = system.actorOf(Props[KafkaActor])

      // Creates HTTP Request and unwraps HttPResponse to get the response json as string
      http.singleRequest(HttpRequest(uri = s"http://localhost:9000/cheapest/${req.src}/${req.des}")).onComplete(response =>
        response.get.entity.dataBytes.runFold(ByteString(""))(_ ++ _).foreach {
          body => req.sender ! body.utf8String
        }
      )
  }

}
