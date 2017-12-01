package com.svntravel.hbase

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.mapreduce.Job

class HbaseConf {

  val conf = HBaseConfiguration.create()
  conf.addResource("/Users/saravandeepak/hbase-1.2.6/conf/hbase-site.xml")
  conf.addResource("/Users/saravandeepak/hbase-1.2.6/conf/hbase-env.sh")





}
