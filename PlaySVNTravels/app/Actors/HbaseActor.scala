package Actors

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}
import akka.http.scaladsl.Http
import akka.stream.{ActorMaterializer, ActorMaterializerSettings}
import hbase.LocationsHbase._

import scala.util.{Failure, Success}

case class HBaseRequest (src:String, dest:String, sender:ActorRef)

class HbaseActor (out:ActorRef)(src:String,des:String) extends Actor with ActorLogging {

  val system = ActorSystem("hbaseSystem")

  val hbaseApiActor = system.actorOf(Props[HbaseApiActor])

  override def receive: Receive = {
    case "hbase" =>
      println("in hbase actor")
      out ! ("received params " + src + " " + des )
      hbaseApiActor ! HBaseRequest(src, des , out)
  }
}

class HbaseApiActor extends Actor {

  val system = ActorSystem("hbaseApiSystem")

  final implicit val materializer: ActorMaterializer = ActorMaterializer(ActorMaterializerSettings(context.system))

  override def receive:Receive = {
    case req:HBaseRequest =>
      println("in Hbase api actor")
      val locations = getLocationsAgg(req.src, req.dest, "2017Q1")
      locations match {
        case Success(x) => req.sender ! x.toString
        case Failure(x) => req.sender ! "Failed retrieving from HBase"
      }

  }
}

object HbaseActor{
  def props(out:ActorRef)(src:String,des:String) = Props(new HbaseActor(out)(src,des))
}
