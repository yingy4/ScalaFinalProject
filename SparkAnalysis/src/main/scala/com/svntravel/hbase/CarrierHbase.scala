package com.svntravel.hbase

import com.svntravel.hbase.HbaseConf.{closeConnections, createConncetion}
import com.svntravel.spark.analysis.{Carrier}
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Put, Table}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.Dataset

object CarrierHbase {

  def addCarrierstoHbase (dsc: Dataset[Carrier], cf: String): Unit = {

    dsc.foreachPartition( {par =>
      val conn  = createConncetion()
      val table = conn.getTable(TableName.valueOf( Bytes.toBytes("carriers")))
      par.foreach(car => putCarriersData(car,table, cf))
      closeConnections(conn,table)
    })

  }

  def putCarriersData (car : Carrier, table: Table, cf:String) : Unit = {

    val put = new Put(Bytes.toBytes(car.carrierCode))
    put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("minFare"), Bytes.toBytes(car.minPrice.toString))
    put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("maxFare"), Bytes.toBytes(car.maxPrice.toString))
    put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("avgFare"), Bytes.toBytes(car.avgPrice.toString))
    put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("stdDev"), Bytes.toBytes(car.stdDev.toString))
    table.put(put)

  }
}
