package utils

import play.api.libs.json.{JsPath, Json, Reads, Writes}
import play.api.libs.functional.syntax._

/**
  *
  * Case Classes for JSON serialization and deserialization
  */
case class AirTerminal(airport: String)
case class BookingInfo(travel_class:String,seats_remaining:Int)
case class Flight(origin: AirTerminal, destination: AirTerminal,marketing_airline:String,booking_info:BookingInfo)
case class InOutbound(flights: List[Flight])
case class Itineraries(outbound: InOutbound, inbound: Option[InOutbound])
case class Fare(total_price: Double)
case class FlightResults(itineraries: List[Itineraries],fare: Fare)
case class CheapFlights(currency: String, results:List[FlightResults])


object CheapFlightsReads {

  implicit val bookingInfoRead: Reads[BookingInfo] = (
    (JsPath \ "travel_class").read[String] and
      (JsPath \ "seats_remaining").read[Int]
    )(BookingInfo.apply _)

  implicit val airTerminalRead: Reads[AirTerminal] =
    (JsPath \ "airport").read[String].map(airport => AirTerminal(airport))

  implicit val flightRead: Reads[Flight] = (
    (JsPath \ "origin").read[AirTerminal] and
      (JsPath \ "destination" ).read[AirTerminal] and
      (JsPath \ "marketing_airline").read[String] and
      (JsPath \ "booking_info").read[BookingInfo]
    ) (Flight.apply _)

  implicit val inOutboundRead: Reads[InOutbound] =
    (JsPath \ "flights").read[List[Flight]].map(flightLst => InOutbound(flightLst))

  implicit val itinerariesRead: Reads[Itineraries] = (
    (JsPath \ "outbound").read[InOutbound] and
      (JsPath \ "inbound").readNullable[InOutbound]
    )(Itineraries.apply _)

  implicit val fareRead: Reads[Fare] =
    (JsPath \ "total_price").read[String].map(totalprice => Fare(totalprice.toDouble))

  implicit val  flightResultsRead: Reads[FlightResults] = (
    (JsPath \ "itineraries").read[List[Itineraries]] and
      (JsPath \ "fare").read[Fare]
    ) (FlightResults.apply _)

  implicit val cheapFlightsRead: Reads[CheapFlights] = (
    (JsPath \ "currency").read[String] and
      (JsPath \ "results").read[List[FlightResults]]
    ) (CheapFlights.apply _)

}

object CheapFlightsWrites {

  implicit val bookingInfoWrites = new Writes[BookingInfo] {
    def writes(bookingInfo: BookingInfo) = Json.obj(
      "travel_class" -> bookingInfo.travel_class,
      "seats_remaining" -> bookingInfo.seats_remaining
    )
  }

  implicit val airTerminalWrites = new Writes[AirTerminal] {
    def writes(terminal: AirTerminal) = Json.obj(
      "airport" -> terminal.airport
    )
  }

  implicit val flightWrites = new Writes[Flight] {
    def writes(flight: Flight) = Json.obj(
      "origin" -> flight.origin,
      "destination" -> flight.destination,
      "marketing_airline" -> flight.marketing_airline,
      "booking_info" -> flight.booking_info
    )
  }

  implicit val inOutBoundWrites = new Writes[InOutbound] {
    def writes(inout: InOutbound) = Json.obj(
      "flights" -> inout.flights
    )
  }

  implicit val intenrariesWrites = new Writes[Itineraries] {
    def writes(itinerary: Itineraries) = Json.obj(
      "outbound" -> itinerary.outbound,
      "inbound" -> itinerary.inbound
    )
  }

  implicit val fareWrites = new Writes[Fare] {
    def writes(fare: Fare) = Json.obj(
      "total_price" -> fare.total_price
    )
  }

  implicit val flightResultsWrites = new Writes[FlightResults] {
    def writes(flightResults: FlightResults) = Json.obj(
      "itineraries" -> flightResults.itineraries,
      "fare" -> flightResults.fare
    )
  }

  implicit val cheapFlightsWriest = new Writes[CheapFlights] {
    def writes(cheapFlights: CheapFlights) = Json.obj(
      "currency" -> cheapFlights.currency,
      "result" -> cheapFlights.results
    )
  }
}




