package com.playstation.test.batch.transform

import com.playstation.test.batch.model.AppProperties
import com.playstation.test.batch.utils.SparkUtil
import org.apache.log4j.{Level, Logger}

/**
 * This table will transform user_details and create final table
 * Author: Krishna Goje
 */
object UserDetailsTransform {

  val inputDb = AppProperties.getInputDb()
  log.setLevel(Level.DEBUG)
  val outputDb = AppProperties.getOutputDb()
  val userDetailsTransformedTable = s"$outputDb.user_info"
  var log: Logger = Logger.getLogger(getClass.getName.stripSuffix("$"))

  def processUserData(): Unit = {
    // query to create final dataset of user data. extracting latest user information based on feed_key if it's already present
    val userDataTransformQuery =
      f"""
         |select feed_key,
         |       user_id,
         |       fav_cuisine,
         |       budget,
         |       ambience,
         |       mar_sta,
         |       address
         |from (
         |         select feed_key,
         |                user_id,
         |                fav_cuisine,
         |                budget,
         |                ambience,
         |                mar_sta,
         |                address,
         |                row_number() over (partition by user_id order by feed_key desc ) as rank
         |         from $inputDb.user_details
         |         having rank=1
         |     ) as a
         |""".stripMargin

    SparkUtil.overwriteTable(userDataTransformQuery, userDetailsTransformedTable)
  }

}
