package hbase

import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Get, Result}
import org.apache.hadoop.hbase.util.Bytes

import scala.util.Try

case class LocationsAgg (origin: String, destination: String, maxFare: Double, minFare: Double, avgFare: Double, stdDev: Double, period: String)

object LocationsHbase {

  def getLocationsAgg(src: String, des: String, cf : String): Try[LocationsAgg] = {
    val conn  = HbaseConf.createConncetion()
    val table = conn.getTable(TableName.valueOf( Bytes.toBytes("location")))
    var get = new Get(Bytes.toBytes(src + " - " + des))
    var result = table.get(get)
    HbaseConf.closeConnections(conn, table)
    import org.apache.hadoop.hbase.util.Bytes
    val familyMap = result.getFamilyMap(Bytes.toBytes(cf))

    val maxFareT = Try(Bytes.toString(familyMap.get(Bytes.toBytes("maxFare"))).toDouble)
    val minFareT = Try((Bytes.toString(familyMap.get(Bytes.toBytes("minFare")))).toDouble)
    val avgFareT = Try(Bytes.toString(familyMap.get(Bytes.toBytes("avgFare"))).toDouble)
    val stdDevT = Try(Bytes.toString(familyMap.get(Bytes.toBytes("stdDev"))).toDouble)

    val res = for {
      maxf <- maxFareT
      minf <- minFareT
      avgf <- avgFareT
      stdf <- stdDevT
    } yield LocationsAgg(src, des, maxf, minf, avgf, stdf, cf)

    res

  }

}
