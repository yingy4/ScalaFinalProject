package Actors

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.stream.{ActorMaterializer, ActorMaterializerSettings}
import akka.util.ByteString
import play.api.libs.json.JsValue


object HttpActor{
  case class getFlight(data:JsValue)
  def props(out:ActorRef) = Props(new HttpActor(out))
}

/** Handles HTTP Requests
  *
  * @param out an Actor reference obj which pipes in and out
  */
//TODO:: remove print statements, introduce logging?
class HttpActor(out:ActorRef) extends Actor with ActorLogging{
  import context.dispatcher

  final implicit val materializer: ActorMaterializer = ActorMaterializer(ActorMaterializerSettings(context.system))
  val http = Http(context.system)
  var maybeQuery: Option[String] = None

  val kafaSystem = ActorSystem()

  override def receive: Receive = {
    case msg:String =>
      val kafkaActor = kafaSystem.actorOf(Props[KafkaActor])
      if(!msg.contains("\\*")){
        http.singleRequest(HttpRequest(uri = s"http://localhost:9000/inspireme/${msg}")).onComplete(response =>
          response.get.entity.dataBytes.runFold(ByteString(""))(_ ++ _).foreach {
            body => kafkaActor ! body.utf8String
          }
        )
      }else{
        val inputArray = msg.split("\\*")
        out ! ("I received your message: " + inputArray(0))
        // Creates HTTP Request and unwraps HttPResponse to get the response json as string
        http.singleRequest(HttpRequest(uri = s"http://localhost:9000/cheapflights/${inputArray(0)}/${inputArray(1)}")).onComplete(response =>
          response.get.entity.dataBytes.runFold(ByteString(""))(_ ++ _).foreach {
            body => kafkaActor ! body.utf8String
          }
        )
      }

  }

}
