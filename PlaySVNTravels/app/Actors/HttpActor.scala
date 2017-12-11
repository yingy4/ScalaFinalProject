package Actors

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.stream.{ActorMaterializer, ActorMaterializerSettings}
import akka.util.ByteString
import play.api.Logger

import scala.collection.mutable.HashMap


case class UserRequest(src:String,dest:String,topic:String,outChannel:ActorRef)
case class InpsiredUserRequest(src:String, topic:String,sender:ActorRef)
case class AggregatedUserRequest(src:String,des:String,sender:ActorRef)

object HttpActor{
  var actorMap = HashMap.empty[String,ActorRef]
  def props(out:ActorRef)(src:String,des:String) = Props(new HttpActor(out)(src,des))
  val system = ActorSystem("httpSystem")
}


/** Handles HTTP Requests
  *
  * @param out an Actor reference obj which pipes in and out
  * @param src the flight source
  * @param des the destination of the flight
  */
//TODO:: remove print statements, introduce logging?
class HttpActor(out:ActorRef)(src:String,des:String) extends Actor with ActorLogging{
import HttpActor.system

  val apiActor = system.actorOf(Props[APIActor])
  override def receive: Receive = {
    case "two" =>
      Logger.debug("case two")
      Logger.debug(s"Http Actor src =${src} and dest = ${des}")
      HttpActor.actorMap.put("out",out)
      apiActor ! UserRequest(src,des,Kafka.AmadeusProducer.TOPIC,out)
    case "one" =>
      Logger.debug("case one")
      Logger.debug(s"Http Actor src =${src}")
      apiActor ! InpsiredUserRequest(src,Kafka.AmadeusProducer.TOPIC,out)
    case "aggregate" =>
      Logger.debug("case aggregate")
      Logger.debug(s"Http Actor src =${src} and dest = ${des}")
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
      Logger.debug("case userRequest")
      val kafkaActor = system.actorOf(Props[KafkaActor])

      // Creates HTTP Request and unwraps HttPResponse to get the response json as string
      http.singleRequest(HttpRequest(uri = s"http://localhost:9000/cheapflights/${req.src}/${req.dest}")).onComplete(response =>
        response.get.entity.dataBytes.runFold(ByteString(""))(_ ++ _).foreach {
          body => kafkaActor ! (req.outChannel.path.toSerializationFormat,body.utf8String)
        }
      )
    case req:InpsiredUserRequest=>
      Logger.debug("case InspiredUserRequest")
      val kafkaActor = system.actorOf(Props[KafkaActor])

      // Creates HTTP Request and unwraps HttPResponse to get the response json as string
      http.singleRequest(HttpRequest(uri = s"http://localhost:9000/inspireme/${req.src}")).onComplete(response =>
        response.get.entity.dataBytes.runFold(ByteString(""))(_ ++ _).foreach {
          body => req.sender ! body.utf8String
        }
      )
    case req:AggregatedUserRequest=>
      Logger.debug("case AggregatedUserRequest")
      val kafkaActor = system.actorOf(Props[KafkaActor])

      // Creates HTTP Request and unwraps HttPResponse to get the response json as string
      http.singleRequest(HttpRequest(uri = s"http://localhost:9000/cheapest/${req.src}/${req.des}")).onComplete(response =>
        response.get.entity.dataBytes.runFold(ByteString(""))(_ ++ _).foreach {
          body => req.sender ! body.utf8String
        }
      )
  }

}
