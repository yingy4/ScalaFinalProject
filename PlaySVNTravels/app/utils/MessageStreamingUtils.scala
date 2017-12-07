package utils

import Kafka.SparkStreamProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import play.api.libs.functional.syntax._
import play.api.libs.json.{JsPath, Json, Reads}



case class AirTerminal(airport: String)
case class BookingInfo(travel_class:String,seats_remaining:Int)
case class Flight(origin: AirTerminal, destination: AirTerminal,marketing_airline:String,booking_info:BookingInfo)
case class InOutbound(flights: List[Flight])
case class Itineraries(outbound: InOutbound, inbound: Option[InOutbound])
case class Fare(total_price: Double)
case class FlightResults(itineraries: List[Itineraries],fare: Fare)
case class CheapFlights(currency: String, results:List[FlightResults])

object MessageStreamingUtils{

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
      import utils.MessagePushingUtils._
      val msg = Json.toJson(rdd.collect())

      //--- Push to Kafa then to UI ----//
      //println(msg.toString())
      SparkStreamProducer.producer.send(new ProducerRecord(SparkStreamProducer.TOPIC,"spark",msg.toString()))
    }
    )

    ssc.start()
    ssc.awaitTermination()
  }
}


object ProcessCheapFlightStream{
    def ReduceCheapFlightToTen(cfs: CheapFlights, n: Int) = {
      if(n < cfs.results.length)
        cfs.results.sortBy( r => r.fare.total_price )
      else
        cfs.results.sortBy( r => r.fare.total_price )
    }
}