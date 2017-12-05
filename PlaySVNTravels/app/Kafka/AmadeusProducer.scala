package Kafka


/***
  * The kafka producer application.
  */
//TODO:: make it a general producer, to handle all API pushes to log?
object AmadeusProducer{
  import java.util.Properties

  import org.apache.kafka.clients.producer._

  val  props = new Properties()
  props.put("bootstrap.servers", "localhost:9092")

  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  val producer = new KafkaProducer[String, String](props)

  val TOPIC="Amadeus"
}