package com.svntravel.spark.analysis

import org.apache.spark.sql.SparkSession
import org.scalatest.{FlatSpec, Matchers}

import scala.util.{Failure, Success, Try}

class AirlinesSpec extends FlatSpec with Matchers {

  implicit val spark = SparkSession
    .builder()
    .appName("Airlines")
    .master("local[*]")
    .getOrCreate()

  behavior of "read CSV"

  it should """Successfully read csv""" in {

    val csv = "/Users/saravandeepak/SCALA/Dataset/Coupon/156943985_T_DB1B_COUPON_2017Q1.csv"

    val dfT = Try(SQL.readCSV(csv,spark))

    val count = for {
      df <- dfT
    } yield df.count()
    count match {
      case Success(x) => x shouldBe 8960506
    }
  }

  it should """return exception for wrong file""" in {

    val csv = "asdasd"

    val df = Try(SQL.readCSV(csv,spark))

    df match {
      case Failure(x) => assert(true)
    }

  }

  behavior of "join"

  it should """give a joined dataset""" in {

    val df1T = Try(SQL.readCSV("/Users/saravandeepak/SCALA/Dataset/Coupon/156943985_T_DB1B_COUPON_2017Q1.csv", spark))

    val df2T = Try(SQL.readCSV("/Users/saravandeepak/SCALA/Dataset/Ticket/392184100_T_DB1B_TICKET_2017Q1.csv", spark))

    val joinDf = for {
      df1 <- df1T
      df2 <- df2T
    } yield Join.joinDataset(df1, df2, "ITIN_ID")

    joinDf match {
      case Success(x) => assert(true)
    }

  }








}
