package Json


import org.scalatest.{FlatSpec, Matchers}
import play.api.libs.json.Json
import utils.CheapFlights


class JsonSpec extends FlatSpec with Matchers {
  val flightStream = """{"currency":"USD","results":[{"itineraries":[{"outbound":{"flights":[{"departs_at":"2017-12-25T21:00","arrives_at":"2017-12-26T08:20","origin":{"airport":"BOS","terminal":"E"},"destination":{"airport":"LGW","terminal":"S"},"marketing_airline":"DY","operating_airline":"DY","flight_number":"7148","aircraft":"789","booking_info":{"travel_class":"ECONOMY","booking_code":"Z","seats_remaining":9}}]}}],"fare":{"total_price":"451.10","price_per_adult":{"total_fare":"451.10","tax":"28.10"},"restrictions":{"refundable":false,"change_penalties":true}}},{"itineraries":[{"outbound":{"flights":[{"departs_at":"2017-12-25T09:25","arrives_at":"2017-12-25T11:38","origin":{"airport":"BOS","terminal":"A"},"destination":{"airport":"YYZ","terminal":"3"},"marketing_airline":"WS","operating_airline":"WS","flight_number":"3603","aircraft":"DH4","booking_info":{"travel_class":"ECONOMY","booking_code":"I","seats_remaining":7}},{"departs_at":"2017-12-25T20:44","arrives_at":"2017-12-26T09:08","origin":{"airport":"YYZ","terminal":"3"},"destination":{"airport":"LGW","terminal":"N"},"marketing_airline":"WS","operating_airline":"WS","flight_number":"3","aircraft":"76W","booking_info":{"travel_class":"ECONOMY","booking_code":"Q","seats_remaining":6}}]}}],"fare":{"total_price":"684.16","price_per_adult":{"total_fare":"684.16","tax":"39.16"},"restrictions":{"refundable":false,"change_penalties":true}}}]}"""
  val flightJson = Json.parse(flightStream)
  import utils.CheapFlightsReads._

   it should  "deserialize to CheapFlights case class" in {
     val objCheapFlights = flightJson.as[CheapFlights]
     assert(objCheapFlights.currency == "USD")
     val frList = objCheapFlights.results
     val itList = frList.head
     val fare = itList.fare
     assert(fare.total_price == 451.10)

  }

  it should "Serialize to CheapFlights case class" in {
    val objCheapFlights = flightJson.as[CheapFlights]

    assert(flightJson.toString() == flightStream)

  }
}
