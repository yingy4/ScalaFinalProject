package Actors

import java.util
import java.util.Properties
import play.api.Logger

import Kafka.SparkStreamProducer
import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import org.apache.kafka.clients.consumer.KafkaConsumer

import scala.collection.JavaConverters._
import scala.collection.mutable.HashMap

object ConsumerActor{
  val system = ActorSystem("kafkaConsumerSystem")
  val keyMap = HashMap.empty[String,String]

}

class ConsumerActor extends Actor{
  val  props = new Properties()
  props.put("bootstrap.servers", "localhost:9092")

  props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("group.id", "kafka-consumer")

  val consumer = new KafkaConsumer[String, String](props)

  consumer.subscribe(util.Collections.singletonList(SparkStreamProducer.TOPIC))
  var running = true
  override def receive = {
    case (key:String,data:String)=>
      Logger.info("In consumer Actor")

      HttpActor.actorMap.get(key) match{
        case Some(actor) => actor ! data
        case None => Logger.debug("No such Key for Http Actor")
      }
    case (msg:String,poll:Boolean) =>
      while(poll) {
        val records = consumer.poll(Long.MaxValue)
        for (record <- records.asScala) {
          val consumerKeyActor = ConsumerActor.system.actorOf(Props[ConsumerKeyActor])
          if(record.value().contains("{")){
            HttpActor.actorMap.get("out") match {
              case Some(out) =>
                Logger.info("data = "+record.value())
                out ! record.value()
              case None => Logger.debug("No such key for Out channel, id ="+record.key())
            }

          }

        }
      }
    case _ => println("some error, not receiving")
  }
}

class ConsumerKeyActor extends Actor{
  override def receive: Receive = {
    case (key:String,actorRefPath:String) =>
      Logger.info("In consumer key Actor")
      val consumerActor = ConsumerActor.system.actorOf(Props[ConsumerActor])
      consumerActor ! ("start",true)

    case (data:String,actor:ActorRef) =>
      HttpActor.actorMap.get("out") match {
        case Some(out) => {
          out ! data
        }
        case None => Logger.debug("No such key in consumer key actor")
      }
  }
}