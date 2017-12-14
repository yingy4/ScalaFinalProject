package controllers

import javax.inject._


import play.api.libs.ws.WSClient
import play.api.mvc._
import play.api.Logger
import play.api.Configuration



@Singleton
class AmadeusController @Inject() (cc :ControllerComponents) (ws: WSClient) (config: Configuration) extends AbstractController(cc){
  import scala.concurrent.ExecutionContext.Implicits._

  val apiKey = config.underlying.getString("amadeuskey")

  val url = "https://api.sandbox.amadeus.com/v1.2/flights/low-fare-search"

  def cheapFlights (src: String, des: String) = Action.async {
    Logger.info("in cheap flights controller action")
    ws.url(url).addQueryStringParameters("apikey" -> apiKey,"origin" -> src,"destination" -> des, "departure_date" -> "2017-12-25").get().map { response =>
      Ok(response.body)
    }
  }

  /***
    * The Inspiration Flight Search allows you to find the prices of one-way and return flights from
    * an origin city without necessarily having a destination, or even a flight date, in mind.
    * The search doesn't return a distinct set of price options, but rather, can tell you the price of
    * flying from a given city to some destination, for a trip of a given duration, that falls
    * within a given date range.
    */

  val inspirationURL = "https://api.sandbox.amadeus.com/v1.2/flights/inspiration-search"
  def inspirationSearch (src: String) = Action.async {
    Logger.info("in inspiration search controller action")
    ws.url(inspirationURL).addQueryStringParameters("apikey" -> apiKey,"origin" -> src).get().map { response =>
      Ok(response.body)
    }
  }

  /**
    * The Extensive Flight Search allows you to find the prices of one-way or return flights between
    * two airports over a large number of dates, and for a large variety of stay durations.
    * The search doesn't return exact itineraries, but rather tells you the best price for a flight
    * on a given day, for a stay of a given duration.
    */

  val aggregationURL = "https://api.sandbox.amadeus.com/v1.2/flights/extensive-search"
  def destinationAgrregation (src: String,des:String) = Action.async {
    Logger.info("in two way aggregation controller action")
    ws.url(aggregationURL).addQueryStringParameters("apikey" -> apiKey,"origin" -> src,"destination" -> des,"aggregation_mode" ->"DESTINATION").get().map { response =>
      Ok(response.body)
    }
  }

  //For one way
  def destinationAgrregationOneWay (src: String,des:String) = Action.async {
    Logger.info("in one way aggregation controller action")
    ws.url(aggregationURL).addQueryStringParameters("apikey" -> apiKey,"origin" -> src,"destination" -> des,"one-way"->"true","aggregation_mode" ->"DESTINATION").get().map { response =>
      Ok(response.body)
    }
  }

}
