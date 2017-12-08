package spark

import Kafka.SparkStreamProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import play.api.libs.json.Json
import utils.CheapFlights


class StreamingCheapFlights {

  val ssc = CommonSparkContext.sparkStreamingContext

  val topics = List("Amadeus").toSet
  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> "localhost:9092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "kafka-amadeus",
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )

  val cheapFlightsStream = KafkaUtils.createDirectStream[String, String](
    ssc,
    LocationStrategies.PreferConsistent,
    ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)).map {
        import utils.CheapFlightsReads._
    record => Json.parse(record.value()).as[CheapFlights]
  }.flatMap(SparkStreamAnalyzer.getCheapestFlights(_ , 10))

  cheapFlightsStream.foreachRDD( rdd => {
    import utils.CheapFlightsWrites._
    val jsflights = Json.toJson(rdd.collect())

    //--- Push to Kafa then to UI ----//
    //println(msg.toString())
    SparkStreamProducer.producer.send(new ProducerRecord(SparkStreamProducer.TOPIC,"spark", jsflights.toString()))
  }
  )

  ssc.start()
  ssc.awaitTermination()
}

