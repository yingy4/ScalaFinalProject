package utils

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import play.api.libs.functional.syntax._
import play.api.libs.json.{JsPath, Json,Reads}



case class AirTerminal(airport: String)
case class Flight(origin: AirTerminal, destination: AirTerminal)
case class InOutbound(flights: List[Flight])
case class Itineraries(outbound: InOutbound, inbound: Option[InOutbound])
case class Fare(total_price: String)
case class FlightResults(itineraries: List[Itineraries],fare: Fare)
case class CheapFlights(currency: String, results:List[FlightResults])

object MessageStreamingUtils{

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
      (JsPath \ "inbound").readNullable[InOutbound]
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

  def streamUtil = {
    val master = "local[*]"
    val appName = "MessageApp"

    val sparkConf = new SparkConf().setMaster(master).setAppName(appName)//.set("spark.driver.allowMultipleContexts", "true")
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    val topics = List("Amadeus").toSet
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "192.168.0.4:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "kafka-test",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val cheapFlightsStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)).map {
      record => Json.parse(record.value()).as[CheapFlights]
    }.flatMap(ProcessCheapFlightStream.ReduceCheapFlightToTen(_ , 10))

    cheapFlightsStream.foreachRDD( rdd => {
      val msg = rdd.collect()
      //--- Push to Kafa then to UI ----//
      //TODO
    }
    )

    ssc.start()
    ssc.awaitTermination()
  }
}


object ProcessCheapFlightStream{
    def ReduceCheapFlightToTen(cfs: CheapFlights, n: Int) = {
      if(n < cfs.results.length)
        cfs.results.sortBy( r => r.fare ).take(n)
      else
        cfs.results.sortBy( r => r.fare )
    }
}