package utils

import play.api.libs.json.{Json, Writes}

/**
  * The utility class which the json library uses to serialize the case class objects to json format
  */
object MessagePushingUtils{

  implicit val airTerminalWrites = new Writes[AirTerminal]{
      def writes(terminal: AirTerminal) = Json.obj(
        "airport" -> terminal.airport
      )
  }

  implicit val flightWrites = new Writes[Flight]{
      def writes(flight:Flight) = Json.obj(
        "origin" -> flight.origin,
        "destination" -> flight.destination
      )
  }

  implicit val inOutBoundWrites = new Writes[InOutbound]{
      def writes(inout:InOutbound) = Json.obj(
        "flights" -> inout.flights
      )
  }

  implicit val intenrariesWrites = new Writes[Itineraries]{
      def writes(itinerary:Itineraries) = Json.obj(
        "outbound" -> itinerary.outbound,
        "inbound" -> itinerary.inbound
      )
  }

  implicit val farWrites = new Writes[Fare]{
      def writes(fare:Fare) = Json.obj(
        "total_price" -> fare.total_price
      )
  }

  implicit val flightResultsWrites = new Writes[FlightResults]{
      def writes(flightResults:FlightResults) = Json.obj(
        "itineraries" -> flightResults.itineraries,
        "fare" -> flightResults.fare
      )
  }

  implicit val cheapFlightsWriest = new Writes[CheapFlights]{
    def writes(cheapFlights:CheapFlights) = Json.obj(
      "currency" -> cheapFlights.currency,
      "result" -> cheapFlights.results
    )
  }
}