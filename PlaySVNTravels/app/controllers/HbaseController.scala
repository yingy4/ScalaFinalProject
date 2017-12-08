package controllers

import javax.inject.{Inject, Singleton}

import Actors.HbaseActor
import akka.actor.ActorSystem
import akka.stream.Materializer
import play.api.libs.streams.ActorFlow
import play.api.libs.ws.WSClient
import play.api.mvc.{AbstractController, ControllerComponents, WebSocket}

import scala.util.{Failure, Success}

@Singleton
class HbaseController @Inject() (cc :ControllerComponents)
                                (ws: WSClient)(implicit system: ActorSystem, mat: Materializer) extends AbstractController(cc){

  def getLocationsData (src: String, des: String) = Action {
    import hbase.LocationsHbase._

    val result = getLocationsAgg(src, des, "2017Q1")

    result match {
      case Success(x) => Ok(x.toString())
      case Failure(x) => Ok("Parse failed")
    }

  }

  def websocket(src:String,dest:String) = WebSocket.accept[String,String]{ request =>
    println(src)
    ActorFlow.actorRef{ out =>
      HbaseActor.props(out)(src, dest)
    }
  }

}
