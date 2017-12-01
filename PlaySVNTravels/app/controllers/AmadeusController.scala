package controllers

import javax.inject._

import play.api.libs.ws.WSClient
import play.api.mvc._



@Singleton
class AmadeusController @Inject() (cc :ControllerComponents) (ws: WSClient) extends AbstractController(cc){
  import scala.concurrent.ExecutionContext.Implicits._

  val url = "https://api.sandbox.amadeus.com/v1.2/flights/low-fare-search"

  def cheapFlights (src: String, des: String) = Action.async {
    ws.url(url).addQueryStringParameters("apikey" -> "Gbv5AzOeVWw2c0R3r2TBdA2SJA4kZkpB","origin" -> src,"destination" -> des, "departure_date" -> "2017-12-25").get().map { response =>
      Ok(response.body)
    }
  }

}
