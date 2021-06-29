package com.playstation.test.batch.question

import com.playstation.test.batch.model.AppProperties
import com.playstation.test.batch.utils.SparkUtil
import org.apache.log4j.{Level, Logger}

/**
 * This object will provide methods for queries
 * Author: Krishna Goje
 */
object Queries {

  val inputDb = AppProperties.getInputDb()
  val outputDb = AppProperties.getOutputDb()

  var log: Logger = Logger.getLogger(getClass.getName.stripSuffix("$"))
  log.setLevel(Level.DEBUG)

  /**
   * This method will provide top 3 restaurant for each cuisine type within given date range
   *
   */
  def top3RestaurantsForCuisine(): Unit = {

    val startDt = "2020-01-01"
    val endDt = "2021-06-28"

    /**
     * Below query will get top 3 restaurants for each cuisine type based on sale_amt.
     *
     * How it works?
     * Stage 1: query will aggregate sum(sale_amt) using window function for each place_id and cuisine type with date filter on partition column
     * Stage 2: query will read output from stage 1 and assign rank using window function paritioned by served_cuisine column and filters top 3 records for each cuisine type
     *
     */
    val top3RestaurantsQuery =
      f"""
         |select feed_key,
         |       user_id,
         |       place_id,
         |       served_cuisine,
         |       visit_tm,
         |       user_rating,
         |       sum_sale_amt,
         |       visit_dt,
         |       rank() over (partition by served_cuisine order by sum_sale_amt desc ) as ranking
         |from (
         |         select feed_key,
         |                user_id,
         |                place_id,
         |                served_cuisine,
         |                visit_tm,
         |                user_rating,
         |                visit_dt,
         |                sum(sale_amt) over (partition by served_cuisine,place_id ) as sum_sale_amt
         |         from $outputDb.restaurant_intr_info a
         |         where visit_dt between '$startDt' and '$endDt'
         |     ) as a
         |having ranking <= 3
         |""".stripMargin

    log.info(top3RestaurantsQuery)
    SparkUtil.sparkSession.sql(top3RestaurantsQuery).collect().foreach(x => {
      log.info(x)
      println(x)
    })

  }

  /**
   * This method will provide top nth record for each cuisine type based on sale_amt
   */
  def topNRestaurant(): Unit = {
    val startDt = "2020-01-01"
    val endDt = "2021-06-28"

    val nthRecord = 6

    /**
     * Below query will get top 3 restaurants for each cuisine type based on sale_amt.
     *
     * How it works?
     * Stage 1: query will aggregate sum(sale_amt) using window function for each place_id and cuisine type with date filter on partition column
     * Stage 2: query will read output from stage 1 and assign rank using window function paritioned by served_cuisine column and filters nth record for each cuisine type
     *
     */
    val topNRestaurantQuery =
      f"""
         |select feed_key,
         |       user_id,
         |       place_id,
         |       served_cuisine,
         |       visit_tm,
         |       user_rating,
         |       sum_sale_amt,
         |       visit_dt,
         |       rank() over (partition by served_cuisine order by sum_sale_amt desc ) as ranking
         |from (
         |         select feed_key,
         |                user_id,
         |                place_id,
         |                served_cuisine,
         |                visit_tm,
         |                user_rating,
         |                visit_dt,
         |                sum(sale_amt) over (partition by served_cuisine,place_id ) as sum_sale_amt
         |         from $outputDb.restaurant_intr_info a
         |         where visit_dt between '$startDt' and '$endDt'
         |     ) as a
         |having ranking = $nthRecord
         |""".stripMargin

    log.info(topNRestaurantQuery)
    SparkUtil.sparkSession.sql(topNRestaurantQuery).collect().foreach(x => {
      log.info(x)
      println(x)
    })

  }

  /**
   * This method will return avg visit time for a user_id for a given date
   */
  def avgVisitTime(): Unit = {
    val visitDt = "2020-01-01"

    val avgVisitTimeQuery =
      f"""
         |select user_id,
         |       coalesce(cast(avg(diff) as int), -9994) as avg_diff
         |from (
         |         select user_id,
         |                unix_timestamp(visit_tm) - unix_timestamp(
         |                        LAG(visit_tm) OVER (PARTITION BY user_id ORDER BY visit_tm asc )) as diff
         |         from $outputDb.restaurant_intr_info
         |         where visit_dt = '$visitDt'
         |     ) as a
         |group by user_id
         |""".stripMargin
    log.info(avgVisitTimeQuery)
    SparkUtil.sparkSession.sql(avgVisitTimeQuery).collect().foreach(x => {
      log.info(x)
      println(x)
    })
  }

}
