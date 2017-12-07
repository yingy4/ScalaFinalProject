package controllers

import javax.inject.{Inject, Singleton}

import play.api.mvc.{AbstractController, ControllerComponents}

import scala.util.{Failure, Success}

@Singleton
class HbaseController @Inject() (cc :ControllerComponents) extends AbstractController(cc){

  def getLocationsData (src: String, des: String) = Action {
    import hbase.LocationsHbase._

    val result = getLocationsAgg(src, des, "2017Q1")

    result match {
      case Success(x) => println(x)
        Ok("Success")
      case Failure(x) => Ok("Parse failed")
    }

  }

}
