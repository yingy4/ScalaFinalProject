package Actors

import Kafka.AmadeusProducer
import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import org.apache.kafka.clients.producer.ProducerRecord
import play.api.Logger


/***
  * Actor to handle kafka. Passes the message to  producer application.
  */

class KafkaActor extends Actor with ActorLogging{
  override def receive: Receive = {
    case amadeusAPI:String =>
      Logger.info("in kafka actor")
      AmadeusProducer.producer.send(new ProducerRecord(AmadeusProducer.TOPIC,"am",amadeusAPI))
    case (key:String,data:String) =>
      Logger.info("in kafka actor, case tuple")
      AmadeusProducer.producer.send(new ProducerRecord(AmadeusProducer.TOPIC,key,data))
    case _ =>Logger.error("Kafa error")
  }
}

object  KafkaActor{
  def props(out:ActorRef) = Props(new KafkaActor())
}