package controllers

import javax.inject._

import Actors.{HttpActor, UserRequest}
import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.util.Timeout
import play.api.libs.streams.ActorFlow
import play.api.libs.ws.WSClient
import play.api.mvc._

import scala.concurrent.duration._

/***
  * Identitical to the AmadeusController except for the websocket logic implementation
  *
  *
  * @param cc The play controller components object
  * @param ws Play frameworks web services client object
  * @param system implicit ActorSystem for websockets
  * @param mat Materializer object for the ActorFlow pipe
  */

@Singleton
class APIController @Inject() (cc :ControllerComponents)
      (ws: WSClient)(implicit system: ActorSystem, mat: Materializer) extends AbstractController(cc){
  import scala.concurrent.ExecutionContext.Implicits._

  implicit val timeout: Timeout = 5.seconds
  val url = "https://api.sandbox.amadeus.com/v1.2/flights/low-fare-search"

  def cheapFlights (src: String, des: String) = Action.async {
          ws.url(url).addQueryStringParameters("apikey" -> "Gbv5AzOeVWw2c0R3r2TBdA2SJA4kZkpB","origin" -> src,"destination" -> des, "departure_date" -> "2017-12-25")
              .get()
              .map { response =>Ok(response.body)}
  }

  /***
    * creates a websocket connection which takes a string (for now) and returns a string
    */


  def websocket(src:String,dest:String) = WebSocket.accept[String,String]{ request =>
    ActorFlow.actorRef { out =>
      HttpActor.props(out)(src,dest)
    }
  }

  def websocketDestinationAgrregation(src:String,des:String) = WebSocket.accept[String,String]{ request =>
    ActorFlow.actorRef {
      out =>HttpActor.props(out)(src,des)
    }
  }

}
