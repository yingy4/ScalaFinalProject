package controllers

import javax.inject._

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils
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

    val sparkConf = new SparkConf().setMaster(master).setAppName(appName).set("spark.driver.allowMultipleContexts", "true")
    val ssc = new StreamingContext(sparkConf, Seconds(10))

    val topics = List("mytopic").toSet
    val kafkaParms = Map[String, String]("metadata.broker.list" -> "localhost:9092")
    val msgStrems = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParms, topics)

    msgStrems.foreachRDD(rdd => {
      val msg = rdd.collect()
      msg.foreach {case (a, b) => println(b)}
    })
    //val getMessage = msgStrems.print()



    ssc.start()
    ssc.awaitTermination()
  }
}



