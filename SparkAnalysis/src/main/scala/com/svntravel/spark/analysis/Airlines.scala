package com.svntravel.spark.analysis

import it.nerdammer.spark.hbase.rddToHBaseBuilder
import org.apache.spark.sql.SparkSession

case class Ticket (itinId: BigInt, year: Int, quarter: Int, origin: String, destination: String, roundTrip: Double, fare: Double, miles: Double, tktCarrier: String, opCarrier: String)

case class Locations (year: Int, quarter: Int, origin: String, destination: String, minPrice: Double, maxPrice: Double, avgPrice: Double, stdDev: Double)

object Airlines extends App {

  implicit val spark = SparkSession
    .builder()
    .appName("Airlines")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  import SQL._

  val couponDf = readCSV("/Users/saravandeepak/SCALA/Dataset/Coupon/156943985_T_DB1B_COUPON_2017Q1.csv", spark)

  val ticketsDF = readCSV("/Users/saravandeepak/SCALA/Dataset/Ticket/392184100_T_DB1B_TICKET_2017Q1.csv", spark)

  val marketDF = readCSV("/Users/saravandeepak/SCALA/Dataset/Market/392218011_T_DB1B_MARKET_2017Q1.csv", spark)

  import Join._

  val ctDf = joinDataset(couponDf,ticketsDF, "ITIN_ID")

  val ctmDf = joinDataset(ctDf, marketDF, "MKT_ID")

  import Filter._

  val ticketDF = filterDataset(ctmDf, couponDf, ticketsDF, marketDF, 30.0)

  val ticketDsArr = ticketDF.map(t => Ticket(t.getLong(0), t.getInt(1), t.getInt(2), t.getString(3), t.getString(4), t.getDouble(5), t.getDouble(6), t.getDouble(7), t.getString(8), t.getString(9)))

  import Aggregation._

  val locationsDsArr = locationAggregates(ticketDsArr).map (l => Locations.create(l.getInt(0), l.getInt(1), l.getString(2), l.getString(3), l.getDouble(4), l.getDouble(5), l.getDouble(6), l.getDouble(7)))

  locationsDsArr.write.format("com.databricks.spark.csv").save("locations")

}

object Locations {
  def create(year: Int, quarter: Int, origin: String, destination: String, minPrice: Double, maxPrice: Double, avgPrice: Double, stdDev: Double): Locations = {
    if (!stdDev.isNaN()) new Locations(year, quarter, origin, destination, minPrice, maxPrice, avgPrice, stdDev)
    else new Locations(year, quarter, origin, destination, minPrice, maxPrice, avgPrice, 0.0)
  }

}










