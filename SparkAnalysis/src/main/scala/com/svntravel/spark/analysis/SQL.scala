package com.svntravel.spark.analysis

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._

object SQL {
  //Read csv files with header and datatype
  def readCSV (fileURI: String, spark: SparkSession): DataFrame = {
    spark.read.option("header", "true").option("inferSchema", "true").csv(fileURI)
  }

}

object Aggregation {

  //get max fare for each route for a quarter
  def aggMaxFarePerLocation (ds: Dataset[Ticket]) : DataFrame = {
    ds.select("year", "quarter", "origin", "destination", "fare")
      .groupBy("year", "quarter", "origin", "destination")
      .max()
  }

  //get min fare for each route for a quarter
  def aggMinFarePerLocation (ds: Dataset[Ticket]) : DataFrame = {
    ds.select("year", "quarter", "origin", "destination", "fare")
      .groupBy("year", "quarter", "origin", "destination")
      .min()
  }

  //get avg fare for each route for a quarter
  def aggAvgFarePerLocation (ds: Dataset[Ticket]) : DataFrame = {
    ds.select("year", "quarter", "origin", "destination", "fare")
      .groupBy("year", "quarter", "origin", "destination")
      .mean()
  }

  //get mean fare for each route for a quarter
  def aggMeanFarePerLocation (ds: Dataset[Ticket]) : DataFrame = {
    ds.select("year", "quarter", "origin", "destination", "fare")
      .groupBy("year", "quarter", "origin", "destination")
      .mean()
  }

  //function which collects all the above aggregation into one dataframe
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

  //calculate aggregates for each carriers
  def carrierAggregates (ds: Dataset[Ticket]) : DataFrame = {
    ds.select("year", "quarter", "opCarrier", "fare")
      .groupBy("year", "quarter", "opCarrier")
      .agg(
        min("fare").as("minFare"),
        max("fare").as("maxFare"),
        avg("fare").as("avgFare"),
        stddev(col("fare").cast("double").as("stdFare"))
      )
  }

  //calculate the top carriers for each location
  def topCarrierPerLocation (ds: Dataset[Ticket]) : DataFrame = {
    ds.select("year", "quarter", "origin", "destination", "opCarrier", "fare")
        .groupBy("year", "quarter", "origin", "destination", "opCarrier")
            .agg(
              count("opCarrier").as("count")
//              ,min("fare").as("minFare")
//              ,max("fare").as("maxFare")
//              ,avg("fare").as("avgFare")
            )
            .orderBy(desc("count"))
  }

  //get top routes for a quarter
  def findTopRoutes (ds: Dataset[Ticket], k: Int) : DataFrame = {
    ds.select("year", "quarter", "origin", "destination").groupBy("year", "quarter", "origin", "destination")
      .count()
      .orderBy(desc("count"))
      .limit(k)
  }

  //get top origins for a quarter
  def findTopOrigin (ds: Dataset[Ticket], k: Int) : DataFrame = {
    ds.select("year", "quarter", "origin").groupBy("year", "quarter", "origin")
      .count()
      .orderBy(desc("count"))
      .limit(k)
  }

  //get top destinations for a quarter
  def findTopDestination (ds: Dataset[Ticket], k: Int) : DataFrame = {
    ds.select("year", "quarter", "destination").groupBy("year", "quarter", "destination")
      .count()
      .orderBy(desc("count"))
      .limit(k)
  }

  //get top ticketing carrier for a quarter
  def findTopTicketingCarrier (ds: Dataset[Ticket], k: Int) : DataFrame = {
    ds.select("year", "quarter", "tktCarrier").groupBy("year", "quarter", "tktCarrier")
      .count()
      .orderBy(desc("count"))
      .limit(k)
  }

  //get top operating carrier for a quarter
  def findTopOperatingCarrier (ds: Dataset[Ticket], k: Int) : DataFrame = {
    ds.select("year", "quarter","opCarrier").groupBy("year", "quarter","opCarrier")
      .count()
      .orderBy(desc("count"))
      .limit(k)
  }

  //get percentages of roundtrip in a quarter
  def roundTripPerc (df: DataFrame, ds:Dataset[Ticket]) : Double = {
    ds.where(ds("roundTrip") === 1.0).count().toDouble / df.count().toDouble
  }

}

object Join {

  //inner join two dataframes with foreign key
  def joinDataset (df1: DataFrame, df2: DataFrame, joinColumn: String): DataFrame = {
    df1.join(df2, df1(joinColumn) === df2(joinColumn), "inner")
  }

}

object Filter {

  //filter the joined dataframes
  //remove unnecessary columns
  def filterDataset (df: DataFrame, couponDf: DataFrame, ticketsDF: DataFrame, marketDF: DataFrame, minFare: Double): DataFrame = {
    df.select(couponDf("ITIN_ID"), couponDf("YEAR") , couponDf("QUARTER"), marketDF("ORIGIN"), marketDF("DEST"), ticketsDF("ROUNDTRIP"), marketDF("MARKET_FARE"), ticketsDF("DISTANCE"), marketDF("TICKET_CARRIER"), marketDF("OPERATING_CARRIER"))
      .where(marketDF("ORIGIN") =!= marketDF("DEST"))
      .where(marketDF("MARKET_FARE") > minFare)
      .where(marketDF("MARKET_FARE") < 3500.00)
      .distinct()
  }

}
