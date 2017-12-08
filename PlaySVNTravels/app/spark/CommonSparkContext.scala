package spark

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object CommonSparkContext {

  lazy val sparkConf = new SparkConf(false).setAppName("svn-travels").setMaster("local[*]")

  val sc = SparkContext.getOrCreate(sparkConf)
  val sparkStreamingContext = new StreamingContext(sc, Seconds(3))

}
