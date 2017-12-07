package com.svntravel.hbase

import com.svntravel.hbase.HbaseConf.{closeConnections, createConncetion, printRow}
import com.svntravel.spark.analysis.{Location, LocationCarrierAgg}
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Get, Put, Table}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.Dataset

object LocationsHbase {

  def addLocationstoHbase (dsl: Dataset[Location], cf: String): Unit = {

    dsl.foreachPartition( {par =>
      val conn  = createConncetion()
      val table = conn.getTable(TableName.valueOf( Bytes.toBytes("location")))
      par.foreach(loc => putLocationsData(loc,table, cf))
      closeConnections(conn,table)
    })

  }

  def putLocationsData (loc : Location, table: Table, cf:String) : Unit = {

    val put = new Put(Bytes.toBytes(loc.origin + " - " + loc.destination))
    put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("minFare"), Bytes.toBytes(loc.minPrice.toString))
    put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("maxFare"), Bytes.toBytes(loc.maxPrice.toString))
    put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("avgFare"), Bytes.toBytes(loc.avgPrice.toString))
    put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("stdDev"), Bytes.toBytes(loc.stdDev.toString))
    table.put(put)

  }

  def getLocations (): Unit = {
    val conn  = createConncetion()
    val table = conn.getTable(TableName.valueOf( Bytes.toBytes("location")))
    var get = new Get(Bytes.toBytes("IND - BHM"))
    var result = table.get(get)
    printRow(result)
    closeConnections(conn, table)
  }

  def addTopCarrierPerLocationtoHbase (dsl: Dataset[LocationCarrierAgg], cf: String): Unit = {

    dsl.foreachPartition( {par =>
      val conn  = createConncetion()
      val table = conn.getTable(TableName.valueOf( Bytes.toBytes("topCarrierPerLocation")))
      par.foreach(loc => putTopCarrierPerLocationData(loc,table, cf))
      closeConnections(conn,table)
    })

  }

  def putTopCarrierPerLocationData (lca : LocationCarrierAgg, table: Table, cf:String) : Unit = {

    val put = new Put(Bytes.toBytes(lca.origin + " - " + lca.destination))
    put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(lca.carrierCode), Bytes.toBytes(lca.count.toString))
    table.put(put)

  }


}
