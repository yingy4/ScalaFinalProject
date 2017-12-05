package controllers

import java.util.Properties
import javax.inject._

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import play.api.mvc._

import scala.concurrent.{ExecutionContext, Future}


/**
 * This controller creates an `Action` to handle HTTP requests to the
 * application's home page.
 */
@Singleton
class HomeController @Inject()(cc: ControllerComponents) (implicit ec: ExecutionContext) extends AbstractController(cc) {

  /**
   * Create an Action to render an HTML page with a welcome message.
   * The configuration in the `routes` file means that this method
   * will be called when the application receives a `GET` request with
   * a path of `/`.
   */
  def index = Action {

    val abc = Future{MessageStreaming.streamUtil}
    Ok(views.html.index(s"Your new application is ready. Your message:"))

  }

}


object MessageStreaming{
  def streamUtil = {
    val master = "local[*]"
    val appName = "MessageApp"

    val sparkConf = new SparkConf().setMaster(master).setAppName(appName)//.set("spark.driver.allowMultipleContexts", "true")
    val ssc = new StreamingContext(sparkConf, Seconds(10))

    val topics = List("Amadeus").toSet
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "kafka-test",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val msgStrems = KafkaUtils.createDirectStream[String, String](
                    ssc,
                    LocationStrategies.PreferConsistent,
                    ConsumerStrategies.Subscribe[String, String](topics, kafkaParams))
    msgStrems.map{
      record => (record.key,record.value)
    } foreachRDD(rdd => {
      val msg = rdd.collect()
      msg.foreach {case (a, b) => println(b);println("--------");println(a)}
    })

    ssc.start()
    ssc.awaitTermination()
  }
}



