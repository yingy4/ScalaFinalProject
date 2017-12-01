package com.svntravel.hbase

import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client._


object HbaseConf {


  def createConncetion (): Connection = {
    val conf = HBaseConfiguration.create()
    val conn = ConnectionFactory.createConnection(conf)
    println("Successful connection")
    conn
  }

  def closeConnections (connection: Connection, table: Table) : Unit = {
    table.close()
    connection.close()
    println("Closed Connection")
  }

  def printRow(result : Result) = {
    val cells = result.rawCells()
    print( Bytes.toString(result.getRow) + " : " )
    for(cell <- cells){
      val col_name = Bytes.toString(CellUtil.cloneQualifier(cell))
      val col_value = Bytes.toString(CellUtil.cloneValue(cell))
      print("(%s,%s) ".format(col_name, col_value))
    }
    println()
  }

}


