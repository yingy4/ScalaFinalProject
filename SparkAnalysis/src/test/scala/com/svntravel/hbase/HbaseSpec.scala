package com.svntravel.hbase

import org.scalatest.{FlatSpec, Matchers}

import scala.util.{Success, Try}


class HbaseSpec extends FlatSpec with Matchers {

  behavior of "hbase"

  it should """Establish a connection""" in {
    val conn = Try(HbaseConf.createConncetion());
    conn match {
      case Success(x) => assert(true);
    }
  }

  it should """get the locations result""" in {
    val result = Try(LocationsHbase.getLocations("SFO - JFK"))
    result match {
      case Success(x) => assert(true)
    }

  }

}
