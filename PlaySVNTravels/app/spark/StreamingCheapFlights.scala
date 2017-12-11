package spark

import Actors.{ConsumerActor, ConsumerKeyActor}
import Kafka.SparkStreamProducer
import akka.actor.{ActorRef, Props}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import play.api.Logger
import play.api.libs.json.Json
import utils.CheapFlights

import scala.collection.mutable.ArrayBuffer

object StreamingCheapFlights{
  val consumerKeyActor = ConsumerActor.system.actorOf(Props[ConsumerKeyActor])
  def startPolling(key:String,actor:ActorRef) = {
    actor ! (key,actor.path.toSerializationFormat)
  }
}

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
    ConsumerStrategies.Subscribe[String, String](topics, kafkaParams))

   val arr = new ArrayBuffer[Char]()
   val key = cheapFlightsStream.map(record=> record.key()).flatMap(_.toString).foreachRDD({
     rdd => arr ++= rdd.collect()
     StreamingCheapFlights.startPolling(arr.mkString,StreamingCheapFlights.consumerKeyActor)
   })

  val analyzedData = cheapFlightsStream.map {
        import utils.CheapFlightsReads._
    record => {
      Json.parse(record.value()).as[CheapFlights]
    }
  }.flatMap(SparkStreamAnalyzer.getCheapestFlights(_ , 10))

  analyzedData.foreachRDD( rdd => {
    import utils.CheapFlightsWrites._
    val jsflights = Json.toJson(rdd.collect())

    Logger.debug("Data sent back to kafka")
    SparkStreamProducer.producer.send(new ProducerRecord(SparkStreamProducer.TOPIC,"spark", jsflights.toString()))
    Thread.sleep(1500)

  }
  )

  ssc.start()
  ssc.awaitTermination()
}

