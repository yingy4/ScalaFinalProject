package Actors

import Kafka.AmadeusProducer
import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import org.apache.kafka.clients.producer.ProducerRecord


/***
  * Actor to handle kafka. Passes the message to  producer application.
  */
//TODO:: check if the kafka log received the message
//TODO:: remove print statements, introduce logging?
class KafkaActor extends Actor with ActorLogging{
  override def receive: Receive = {
    case amadeusAPI:String =>
      println("in kafka actor")
      AmadeusProducer.producer.send(new ProducerRecord(AmadeusProducer.TOPIC,"am",amadeusAPI))
    case _ => println("error at kafka actor")
  }
}

//TODO:: not really needed.
object  KafkaActor{
  def props(out:ActorRef) = Props(new KafkaActor())
}