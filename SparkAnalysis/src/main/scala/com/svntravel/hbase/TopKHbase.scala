package com.svntravel.hbase

import com.svntravel.hbase.HbaseConf.{closeConnections, createConncetion}
import com.svntravel.spark.analysis._
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.Dataset
import org.apache.hadoop.hbase.client.{Get, Put, Table}

object TopKHbase {

  def addTopRoutesToHbase (dsr: Dataset[Routes], cf: String) : Unit = {

    dsr.foreachPartition({ par =>
      val conn  = createConncetion()
      val table = conn.getTable(TableName.valueOf( Bytes.toBytes("topRoutes")))
      par.foreach(route => putTopRoutesData(route,table, cf))
      closeConnections(conn,table)
    })
  }

  def putTopRoutesData (route: Routes, table: Table, cf: String) : Unit = {

    val put = new Put(Bytes.toBytes(route.origin + " - " + route.destination))
    put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("origin"), Bytes.toBytes(route.origin))
    put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("destination"), Bytes.toBytes(route.destination))
    put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("count"), Bytes.toBytes(route.count.toString))
    table.put(put)
  }

  def addTopOriginsToHbase (dso: Dataset[Origin], cf: String) : Unit = {

    dso.foreachPartition({ par =>
      val conn  = createConncetion()
      val table = conn.getTable(TableName.valueOf( Bytes.toBytes("topOrigins")))
      par.foreach(org => putTopOriginData(org,table, cf))
      closeConnections(conn,table)
    })
  }

  def putTopOriginData (org: Origin, table: Table, cf: String) : Unit = {

    val put = new Put(Bytes.toBytes(org.origin))
    put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("origin"), Bytes.toBytes(org.origin))
    put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("count"), Bytes.toBytes(org.count.toString))
    table.put(put)
  }

  def addTopDestinationsToHbase (dsd: Dataset[Destination], cf: String) : Unit = {

    dsd.foreachPartition({ par =>
      val conn  = createConncetion()
      val table = conn.getTable(TableName.valueOf( Bytes.toBytes("topDestinations")))
      par.foreach(des => putTopDestinationData(des,table, cf))
      closeConnections(conn,table)
    })
  }

  def putTopDestinationData (des: Destination, table: Table, cf: String) : Unit = {

    val put = new Put(Bytes.toBytes(des.destination))
    put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("destination"), Bytes.toBytes(des.destination))
    put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("count"), Bytes.toBytes(des.count.toString))
    table.put(put)
  }

  def addTopOperatingCarrierToHbase (dsoc: Dataset[OperatingCarrier], cf: String) : Unit = {

    dsoc.foreachPartition({ par =>
      val conn  = createConncetion()
      val table = conn.getTable(TableName.valueOf( Bytes.toBytes("topOperatingCarriers")))
      par.foreach(oc => putTopOperatingCarrierData(oc,table, cf))
      closeConnections(conn,table)
    })
  }

  def putTopOperatingCarrierData (oc: OperatingCarrier, table: Table, cf: String) : Unit = {

    val put = new Put(Bytes.toBytes(oc.carrierCode))
    put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("OperatingCarrier"), Bytes.toBytes(oc.carrierCode))
    put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("count"), Bytes.toBytes(oc.count.toString))
    table.put(put)
  }

  def addTopTicketingCarrierToHbase (dstc: Dataset[TicketingCarrier], cf: String) : Unit = {

    dstc.foreachPartition({ par =>
      val conn  = createConncetion()
      val table = conn.getTable(TableName.valueOf( Bytes.toBytes("topTicketingCarriers")))
      par.foreach(tc => putTopTicketingCarrierData(tc,table, cf))
      closeConnections(conn,table)
    })
  }

  def putTopTicketingCarrierData (tc: TicketingCarrier, table: Table, cf: String) : Unit = {

    val put = new Put(Bytes.toBytes(tc.carrierCode))
    put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("TicketingCarrier"), Bytes.toBytes(tc.carrierCode))
    put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("count"), Bytes.toBytes(tc.count.toString))
    table.put(put)
  }


}
