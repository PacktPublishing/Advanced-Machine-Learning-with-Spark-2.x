package com.tomekl007.sparkstreaming.transactions

object ItemProductEnricher {
  def enrich: (String) => TopSeller = callEnrichRestService

  /**
    * In real-word scenario it would be a call to the rest service
    **/
  private def callEnrichRestService(itemId: String): TopSeller = {
    TopSeller(itemId, "some_description", 10.0)
  }


}
