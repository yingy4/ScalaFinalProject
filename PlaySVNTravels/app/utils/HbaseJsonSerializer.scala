package utils

import hbase.{LocationCarrierAgg, LocationsAgg, TopCarriers}
import play.api.libs.json.{JsValue, Json, Writes}

object HbaseJsonSerializer {

  implicit val locationWrites = new Writes[LocationsAgg]{
    def writes(loc:LocationsAgg) = Json.obj(
      "origin" -> loc.origin,
      "destination" -> loc.destination,
      "maxFare" -> loc.maxFare,
      "minFare" -> loc.minFare,
      "avgFare" -> loc.avgFare,
      "stdDev" -> loc.stdDev,
      "period" -> loc.period
    )
  }

  implicit val carrierTupleWrites = new Writes[TopCarriers] {
    def writes(tc: TopCarriers) = Json.obj(
      "carrierCode" -> tc.carrierCode,
      "count" -> tc.count
    )
  }

  implicit val locationCarrierWrites = new Writes[LocationCarrierAgg] {
    def writes(lc: LocationCarrierAgg) = Json.obj(
      "origin" -> lc.origin,
      "destination" -> lc.destination,
      "carriers" -> lc.carrierCode
    )
  }


}
