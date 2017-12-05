package hbase

import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.util.Bytes

object LocationsHbase {

  def getLocations (src: String, des: String): Unit = {
    val conn  = HbaseConf.createConncetion()
    val table = conn.getTable(TableName.valueOf( Bytes.toBytes("locations")))
    var get = new Get(Bytes.toBytes(src + " - " + des))
    var result = table.get(get)
    HbaseConf.printRow(result, "2017Q1")
    HbaseConf.closeConnections(conn, table)
  }

}
