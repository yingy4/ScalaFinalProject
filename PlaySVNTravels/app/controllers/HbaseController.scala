package controllers

import javax.inject.{Inject, Singleton}

import play.api.mvc.{AbstractController, ControllerComponents}

@Singleton
class HbaseController @Inject() (cc :ControllerComponents) extends AbstractController(cc){

  def getLocationsData (src: String, des: String) = Action {
    import hbase.LocationsHbase._

    getLocations(src, des)

    Ok("Success")
  }

}
