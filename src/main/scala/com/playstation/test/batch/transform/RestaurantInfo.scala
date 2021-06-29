package com.playstation.test.batch.transform

import com.playstation.test.batch.utils.SparkUtil
import org.apache.log4j.{Level, Logger}

/**
 * This object will transform and process restaurant information table
 * Author: Krishna Goje
 * */
object RestaurantInfo {

  val inputDb = "kgoje_in"
  log.setLevel(Level.DEBUG)
  val outputDb = "kgoje_out"
  val restaurantInfoTransformedTable = s"$outputDb.restaurant_info"
  var log: Logger = Logger.getLogger(getClass.getName.stripSuffix("$"))

  def restaurantInfoTransform(): Unit = {

    /**
     * Below query will read place_details ingestion table and explodes served_cuisines list into multiple rows
     */
    val restaurantInfoQuery =
      f"""
         |select distinct feed_key,
         |                place_id,
         |                accepted_pymt,
         |                a.served_cuisine as served_cuisine,
         |                open_hrs,
         |                parking_typ
         |from $inputDb.place_details
         |    LATERAL VIEW explode(served_cuisine) a as served_cuisine
         |""".stripMargin

    SparkUtil.overwriteTable(restaurantInfoQuery, restaurantInfoTransformedTable)

  }

}
