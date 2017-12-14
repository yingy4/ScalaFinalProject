package spark

import utils.CheapFlights

object SparkStreamAnalyzer {

  def getCheapestFlights(cfs: CheapFlights, n: Int) = {
    if(n < cfs.results.length)
      cfs.results.sortBy( r => r.fare.total_price ).take(n)
    else
      cfs.results.sortBy( r => r.fare.total_price )
  }

}
