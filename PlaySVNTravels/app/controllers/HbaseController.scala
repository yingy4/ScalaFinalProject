package controllers

import javax.inject.{Inject, Singleton}

import Actors.HbaseActor
import akka.actor.ActorSystem
import akka.stream.Materializer
import hbase.LocationsAgg
import hbase.LocationsHbase.getLocationsAgg
import play.api.libs.streams.ActorFlow
import play.api.libs.ws.WSClient
import play.api.mvc.{AbstractController, ControllerComponents, WebSocket}
import play.api.Logger
import play.api.libs.json.Json

import scala.util.{Failure, Success}

@Singleton
class HbaseController @Inject() (cc :ControllerComponents)
                                (ws: WSClient)(implicit system: ActorSystem, mat: Materializer) extends AbstractController(cc){



  def getLocationsData (src: String, des: String) = Action {
    import hbase.LocationsHbase._

    import utils.HbaseJsonSerializer._
    val result = getLocationsAgg(src, des, "2017Q1")
    result match {
      case Success(x) => Ok(Json.toJson(x))
      case Failure(x) => Ok("parse error")
    }



  }

  def websocket(src:String,dest:String) = WebSocket.accept[String,String]{ request =>
    Logger.debug(s"hbase web socket : src = ${src} and des = ${dest}")
    ActorFlow.actorRef{ out =>
      HbaseActor.props(out)(src, dest)
    }
  }

}
