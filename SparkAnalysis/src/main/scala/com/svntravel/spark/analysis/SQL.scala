package com.svntravel.spark.analysis

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._

object SQL {

  def readCSV (fileURI: String, spark: SparkSession): DataFrame = {
    spark.read.option("header", "true").option("inferSchema", "true").csv(fileURI)
  }

}

object Aggregation {

  def aggMaxFarePerLocation (ds: Dataset[Ticket]) : DataFrame = {
    ds.select("year", "quarter", "origin", "destination", "fare")
      .groupBy("year", "quarter", "origin", "destination")
      .max()
  }

  def aggMinFarePerLocation (ds: Dataset[Ticket]) : DataFrame = {
    ds.select("year", "quarter", "origin", "destination", "fare")
      .groupBy("year", "quarter", "origin", "destination")
      .min()
  }

  def aggAvgFarePerLocation (ds: Dataset[Ticket]) : DataFrame = {
    ds.select("year", "quarter", "origin", "destination", "fare")
      .groupBy("year", "quarter", "origin", "destination")
      .mean()
  }

  def aggMeanFarePerLocation (ds: Dataset[Ticket]) : DataFrame = {
    ds.select("year", "quarter", "origin", "destination", "fare")
      .groupBy("year", "quarter", "origin", "destination")
      .mean()
  }

  def locationAggregates (ds: Dataset[Ticket]) : DataFrame = {
    ds.select("year", "quarter", "origin", "destination", "fare")
      .groupBy("year", "quarter", "origin", "destination")
      .agg(
        min("fare").as("minFare"),
        max("fare").as("maxFare"),
        avg("fare").as("avgFare"),
        stddev(col("fare").cast("double").as("stdFare"))
      )
  }

  def findTopRoutes (ds: Dataset[Ticket], k: Int) : DataFrame = {
    ds.select("year", "quarter", "origin", "destination").groupBy("year", "quarter", "origin", "destination")
      .count()
      .orderBy(desc("count"))
      .limit(k)
  }

  def findTopOrigin (ds: Dataset[Ticket], k: Int) : DataFrame = {
    ds.select("year", "quarter", "origin").groupBy("year", "quarter", "origin")
      .count()
      .orderBy(desc("count"))
      .limit(k)
  }

  def findTopDestination (ds: Dataset[Ticket], k: Int) : DataFrame = {
    ds.select("year", "quarter", "destination").groupBy("year", "quarter", "destination")
      .count()
      .orderBy(desc("count"))
      .limit(k)
  }

  def findTopTicketingCarrier (ds: Dataset[Ticket], k: Int) : DataFrame = {
    ds.select("year", "quarter", "tktCarrier").groupBy("year", "quarter", "tktCarrier")
      .count()
      .orderBy(desc("count"))
      .limit(k)
  }

  def findTopOperatingCarrier (ds: Dataset[Ticket], k: Int) : DataFrame = {
    ds.select("year", "quarter","opCarrier").groupBy("year", "quarter","opCarrier")
      .count()
      .orderBy(desc("count"))
      .limit(k)
  }

  def roundTripPerc (df: DataFrame, ds:Dataset[Ticket]) : Double = {
    ds.where(ds("roundTrip") === 1.0).count().toDouble / df.count().toDouble
  }

}

object Join {

  def joinDataset (df1: DataFrame, df2: DataFrame, joinColumn: String): DataFrame = {
    df1.join(df2, df1(joinColumn) === df2(joinColumn), "inner")
  }

}

object Filter {

  def filterDataset (df: DataFrame, couponDf: DataFrame, ticketsDF: DataFrame, marketDF: DataFrame, minFare: Double): DataFrame = {
    df.select(couponDf("ITIN_ID"), couponDf("YEAR") , couponDf("QUARTER"), marketDF("ORIGIN"), marketDF("DEST"), ticketsDF("ROUNDTRIP"), ticketsDF("ITIN_FARE"), ticketsDF("DISTANCE"), marketDF("TICKET_CARRIER"), marketDF("OPERATING_CARRIER"))
      .where(marketDF("ORIGIN") =!= marketDF("DEST"))
      .where(ticketsDF("ITIN_FARE") > minFare)
      .distinct()
  }

}
