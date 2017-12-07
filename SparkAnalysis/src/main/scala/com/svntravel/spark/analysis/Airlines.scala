package com.svntravel.spark.analysis

import org.apache.spark.sql.{Dataset, SparkSession}
import com.svntravel.hbase.LocationsHbase._
import com.svntravel.hbase.TopKHbase._
import Aggregation._
import com.svntravel.spark.analysis.Airlines.spark


case class Ticket (itinId: BigInt, year: Int, quarter: Int, origin: String, destination: String, roundTrip: Double, fare: Double, miles: Double, tktCarrier: String, opCarrier: String)

case class Location (year: Int, quarter: Int, origin: String, destination: String, minPrice: Double, maxPrice: Double, avgPrice: Double, stdDev: Double)

case class Routes (origin: String, destination: String, count: Long)

case class Origin (origin: String, count: Long)

case class Destination (destination: String, count: Long)

case class OperatingCarrier (carrierCode: String, count: Long)

case class TicketingCarrier (carrierCode: String, count: Long)

case class Carrier (year: Int, quarter: Int, carrierCode: String, minPrice: Double, maxPrice: Double, avgPrice: Double, stdDev: Double)

case class LocationCarrierAgg (year: Int, quarter: Int, origin: String, destination: String, carrierCode: String, count: Long)

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

//  import Location._
//
//  aggLocations(ticketDsArr, "2017Q1")


//  Routes.aggRoutes(ticketDsArr, 100, "2017Q1")
//
//  Origin.aggOrigin(ticketDsArr, 100, "2017Q1")
//
//  Destination.aggDestination(ticketDsArr, 100, "2017Q1")
//
//  OperatingCarrier.aggOperatingCarrier(ticketDsArr, 25, "2017Q1")
//
//  TicketingCarrier.aggTicketingCarrier(ticketDsArr, 25, "2017Q1")

//  Carrier.aggCarrier(ticketDsArr, "2017Q1")

//  LocationCarrierAgg.aggCarrierForLocation(ticketDsArr, "2017Q1")

//  topCarrierPerLocation(ticketDsArr).show(1000)
  
//  println(topCarrierPerLocation(ticketDsArr).count())

}

object Location {

  def create(year: Int, quarter: Int, origin: String, destination: String, minPrice: Double, maxPrice: Double, avgPrice: Double, stdDev: Double): Location = new Location(year, quarter, origin, destination, minPrice, maxPrice, avgPrice, stdDev match { case x if x.isNaN() => 0.0; case _ => stdDev})

  def aggLocations (dst: Dataset[Ticket], cf:String): Unit = {
    import spark.implicits._
    val locationsDsArr = locationAggregates(dst).map (l => Location.create(l.getInt(0), l.getInt(1), l.getString(2), l.getString(3), l.getDouble(4), l.getDouble(5), l.getDouble(6), l.getDouble(7)))
    addLocationstoHbase(locationsDsArr, cf)
  }
}

object Routes {

  def aggRoutes (dst: Dataset[Ticket], k: Int, cf:String): Unit = {
    import spark.implicits._
    val routesDsArr = findTopRoutes(dst, k).map( r => Routes(r.getString(2), r.getString(3), r.getLong(4)))
    addTopRoutesToHbase(routesDsArr,cf)
  }
}

object Origin {

  def aggOrigin (dst: Dataset[Ticket], k: Int, cf:String): Unit = {
    import spark.implicits._
    val originDsArr = findTopOrigin(dst, k).map(o => Origin(o.getString(2),o.getLong(3)))
    addTopOriginsToHbase(originDsArr,cf)
  }
}

object Destination {

  def aggDestination (dst: Dataset[Ticket], k: Int, cf:String): Unit = {
    import spark.implicits._
    val destinationDsArr = findTopDestination(dst, k).map(d => Destination(d.getString(2),d.getLong(3)))
    addTopDestinationsToHbase(destinationDsArr,cf)
  }
}

object OperatingCarrier {

  def aggOperatingCarrier (dst: Dataset[Ticket], k: Int, cf:String): Unit = {
    import spark.implicits._
    val operatingCarrierDsArr = findTopOperatingCarrier(dst, k).map( oc => OperatingCarrier(oc.getString(2), oc.getLong(3)))
    addTopOperatingCarrierToHbase(operatingCarrierDsArr, cf)
  }
}

object TicketingCarrier {

  def aggTicketingCarrier (dst: Dataset[Ticket], k: Int, cf:String): Unit = {
    import spark.implicits._
    val ticketingCarrierDsArr = findTopTicketingCarrier(dst, k).map(tc => TicketingCarrier(tc.getString(2), tc.getLong(3)))
    addTopTicketingCarrierToHbase(ticketingCarrierDsArr, cf)
  }

}

object Carrier {
  import com.svntravel.hbase.CarrierHbase._

  def create(year: Int, quarter: Int, carrierCode: String, minPrice: Double, maxPrice: Double, avgPrice: Double, stdDev: Double): Carrier = new Carrier(year, quarter, carrierCode, minPrice, maxPrice, avgPrice, stdDev match { case x if x.isNaN() => 0.0; case _ => stdDev})

  def aggCarrier (dst: Dataset[Ticket], cf:String): Unit = {
    import spark.implicits._
    val carrierDsArr = carrierAggregates(dst).map(c => Carrier(c.getInt(0), c.getInt(1), c.getString(2), c.getDouble(3), c.getDouble(4), c.getDouble(5), c.getDouble(6)))
    addCarrierstoHbase(carrierDsArr, cf)
  }

}

object LocationCarrierAgg {

  def aggCarrierForLocation (dst: Dataset[Ticket], cf: String): Unit = {
    import spark.implicits._
    val carrierLocationDsArr = topCarrierPerLocation(dst).map(cl => LocationCarrierAgg(cl.getInt(0),cl.getInt(1), cl.getString(2), cl.getString(3), cl.getString(4), cl.getLong(5)))
    addTopCarrierPerLocationtoHbase(carrierLocationDsArr, cf)
  }
}

















