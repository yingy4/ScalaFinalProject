package Actors

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}
import akka.stream.{ActorMaterializer, ActorMaterializerSettings}
import hbase.LocationsHbase._
import play.api.libs.json.Json
import play.api.Logger

import scala.util.{Failure, Success}

case class LocationRequest (src:String, dest:String, sender:ActorRef)
case class LocationCarrierRequest (src:String, dest:String, sender:ActorRef)

class HbaseActor (out:ActorRef)(src:String,des:String) extends Actor with ActorLogging {

  val system = ActorSystem("hbaseSystem")

  val hbaseApiActor = system.actorOf(Props[HbaseApiActor])

  override def receive: Receive = {
    case "locations" =>
      Logger.info("in hbase actor: Locations")
      out ! ("received params " + src + " " + des )
      hbaseApiActor ! LocationRequest(src, des , out)
    case "locationCarrier" =>
      Logger.info("in hbase actor: Location Carrier")
      out ! ("received params " + src + " " + des )
      hbaseApiActor ! LocationCarrierRequest(src, des, out)
  }
}

class HbaseApiActor extends Actor {

  val system = ActorSystem("hbaseApiSystem")

  final implicit val materializer: ActorMaterializer = ActorMaterializer(ActorMaterializerSettings(context.system))

  import utils.HbaseJsonSerializer._
  override def receive:Receive = {
    case req:LocationRequest =>
      Logger.info("in hbase actor: LocationRequest")
      val locations = getLocationsAgg(req.src, req.dest, "2017Q1")
      locations match {
        case Success(x) => req.sender ! Json.stringify(Json.toJson(x))
        case Failure(x) => req.sender ! "Failed retrieving from HBase"
      }

    case req:LocationCarrierRequest =>
      Logger.info("in hbase actor: LocationCarrierRequest")
      val locationsCarrier = getTopCarrierPerLocationData(req.src, req.dest, "2017Q1")
      req.sender ! Json.stringify(Json.toJson(locationsCarrier))

  }
}

object HbaseActor{
  def props(out:ActorRef)(src:String,des:String) = Props(new HbaseActor(out)(src,des))
}
