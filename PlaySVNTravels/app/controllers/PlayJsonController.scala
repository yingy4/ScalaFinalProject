//TODO:: Remove this before final submission
package controllers

import javax.inject.Inject

import play.api.libs.functional.syntax._
import play.api.libs.json.{JsPath, Json, Reads, Writes}
import play.api.mvc.{AbstractController, ControllerComponents}

import scala.concurrent.ExecutionContext

case class AirTerminal(airport: String)
case class Flight(origin: AirTerminal, destination: AirTerminal)
case class InOutbound(flights: List[Flight])
case class Itineraries(outbound: InOutbound, inbound: InOutbound)
case class Fare(total_price: String)
case class FlightResults(itineraries: List[Itineraries],fare: Fare)
case class CheapFlights(currency: String, results:List[FlightResults])

class PlayJsonController @Inject()(cc: ControllerComponents) (implicit ec: ExecutionContext) extends AbstractController(cc) {

  implicit val airTerminalRead: Reads[AirTerminal] =
    (JsPath \ "airport").read[String].map(airport => AirTerminal(airport))

  implicit val flightRead: Reads[Flight] = (
    (JsPath \ "origin").read[AirTerminal] and
      (JsPath \ "destination" ).read[AirTerminal]
  ) (Flight.apply _)

  implicit val inOutboundRead: Reads[InOutbound] =
    (JsPath \ "flights").read[List[Flight]].map(flightLst => InOutbound(flightLst))

  implicit val itinerariesRead: Reads[Itineraries] = (
    (JsPath \ "outbound").read[InOutbound] and
      (JsPath \ "inbound").read[InOutbound]
  )(Itineraries.apply _)

  implicit val fareRead: Reads[Fare] =
    (JsPath \ "total_price").read[String].map(totalprice => Fare(totalprice))

  implicit val  flightResultsRead: Reads[FlightResults] = (
    (JsPath \ "itineraries").read[List[Itineraries]] and
      (JsPath \ "fare").read[Fare]
  ) (FlightResults.apply _)

  implicit val cheapFlightsRead: Reads[CheapFlights] = (
    (JsPath \ "currency").read[String] and
      (JsPath \ "results").read[List[FlightResults]]
  ) (CheapFlights.apply _)

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



  def cJson = Action {

    val inStream = """{
      "currency": "USD",
      "results": [
      {
        "itineraries": [
        {
          "outbound": {
            "flights": [
          {
            "departs_at": "2017-12-25T21:00",
            "arrives_at": "2017-12-26T08:20",
            "origin": {
            "airport": "BOS",
            "terminal": "E"
          },
            "destination": {
            "airport": "LGW",
            "terminal": "S"
          },
            "marketing_airline": "DY",
            "operating_airline": "DY",
            "flight_number": "7148",
            "aircraft": "789",
            "booking_info": {
            "travel_class": "ECONOMY",
            "booking_code": "Z",
            "seats_remaining": 9
          }
          }
            ]
          },
          "inbound": {
            "flights": [
          {
            "departs_at": "2017-12-28T16:00",
            "arrives_at": "2017-12-28T18:30",
            "origin": {
            "airport": "LGW",
            "terminal": "S"
          },
            "destination": {
            "airport": "BOS",
            "terminal": "E"
          },
            "marketing_airline": "DY",
            "operating_airline": "DY",
            "flight_number": "7147",
            "aircraft": "789",
            "booking_info": {
            "travel_class": "ECONOMY",
            "booking_code": "Z",
            "seats_remaining": 6
          }
          }
            ]
          }
        }
        ],
        "fare": {
          "total_price": "1027.26",
          "price_per_adult": {
          "total_fare": "1027.26",
          "tax": "181.26"
        },
          "restrictions": {
          "refundable": false,
          "change_penalties": true
        }
        }
      },
      {
        "itineraries": [
        {
          "outbound": {
            "flights": [
          {
            "departs_at": "2017-12-25T18:25",
            "arrives_at": "2017-12-26T05:20",
            "origin": {
            "airport": "BOS",
            "terminal": "C"
          },
            "destination": {
            "airport": "DUB",
            "terminal": "2"
          },
            "marketing_airline": "EI",
            "operating_airline": "EI",
            "flight_number": "136",
            "aircraft": "330",
            "booking_info": {
            "travel_class": "ECONOMY",
            "booking_code": "K",
            "seats_remaining": 5
          }
          },
          {
            "departs_at": "2017-12-26T15:20",
            "arrives_at": "2017-12-26T16:50",
            "origin": {
            "airport": "DUB",
            "terminal": "2"
          },
            "destination": {
            "airport": "LHR",
            "terminal": "2"
          },
            "marketing_airline": "EI",
            "operating_airline": "EI",
            "flight_number": "172",
            "aircraft": "320",
            "booking_info": {
            "travel_class": "ECONOMY",
            "booking_code": "W",
            "seats_remaining": 4
          }
          }
            ]
          },
          "inbound": {
            "flights": [
          {
            "departs_at": "2017-12-28T07:50",
            "arrives_at": "2017-12-28T09:10",
            "origin": {
            "airport": "LHR",
            "terminal": "2"
          },
            "destination": {
            "airport": "DUB",
            "terminal": "2"
          },
            "marketing_airline": "EI",
            "operating_airline": "EI",
            "flight_number": "151",
            "aircraft": "320",
            "booking_info": {
            "travel_class": "ECONOMY",
            "booking_code": "A",
            "seats_remaining": 9
          }
          },
          {
            "departs_at": "2017-12-28T16:20",
            "arrives_at": "2017-12-28T18:40",
            "origin": {
            "airport": "DUB",
            "terminal": "2"
          },
            "destination": {
            "airport": "BOS",
            "terminal": "C"
          },
            "marketing_airline": "EI",
            "operating_airline": "EI",
            "flight_number": "139",
            "aircraft": "332",
            "booking_info": {
            "travel_class": "ECONOMY",
            "booking_code": "H",
            "seats_remaining": 3
          }
          }
            ]
          }
        }
        ],
        "fare": {
          "total_price": "1825.03",
          "price_per_adult": {
          "total_fare": "1825.03",
          "tax": "335.03"
        },
          "restrictions": {
          "refundable": false,
          "change_penalties": true
        }
        }
      }
      ]
    }"""

   val jsonStream = Json.parse(inStream)
   val cheapFlights = jsonStream.as[CheapFlights]
   val flightResults = cheapFlights.results

    val json = Json.toJson(flightResults)

   println(json.toString())
    //cheapFlights.results.foreach(f => println(f.fare.total_price) )
   // places.residents.foreach(f => println(s"${f.name}") )

    Ok("Done")
  }

}

