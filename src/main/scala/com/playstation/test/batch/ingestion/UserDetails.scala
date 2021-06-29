package com.playstation.test.batch.ingestion

import com.playstation.test.batch.model.AppProperties
import com.playstation.test.batch.utils.{DateUtil, SparkUtil}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.Column
import org.apache.spark.sql.types.{ArrayType, StringType, StructType}

import scala.collection.mutable.ListBuffer

/**
 * This object provides methods to read userDetails json file and interpret data
 */
object UserDetails {

  val userDetailsJsonFile = AppProperties.getUserDetailsJsonFile()
  log.setLevel(Level.DEBUG)
  val inputDb = "kgoje_in"
  val tempDb = "kgoje_temp"
  var log: Logger = Logger.getLogger(getClass.getName.stripSuffix("$"))

  /**
   * Below method will run every time a new json file is ingested for userDetails
   */
  def processUserDetailsData(): Unit = {

    val userDetailsInputView = "user_details"
    val placeInteractionDetailsView = "place_interaction_details"

    val userDetailsTable = s"$inputDb.user_details"
    val placeInteractionDetailsTable = s"$inputDb.place_interaction_details"
    val placeInteractionPartitionsTable = s"$tempDb.place_interaction_partitions"

    // get julian date for current date
    val currJulianDate = DateUtil.getCurrentDate("YYYYDDD")

    // define schema for reading userDetails.json input file
    val userDetailsSchema = new StructType()
      .add("userId", StringType, true)
      .add("favouriteCuisine", ArrayType(StringType), true)
      .add("budget", StringType, true)
      .add("ambience", StringType, true)
      .add("maritalStatus", StringType, true)
      .add("address", StringType, true)
      .add("placeInteractionDetails", ArrayType(
        new StructType()
          .add("placeId", StringType, true)
          .add("visitDate", StringType, true)
          .add("rating", StringType, true)
          .add("saleAmount", StringType, true)
      ))
    //invoke readJson wrapper to read json file and create view
    SparkUtil.readJson(userDetailsJsonFile, userDetailsSchema, true, userDetailsInputView)

    val userDetails =
      f"""
         |select userId            as user_id,
         |       favouriteCuisine  as fav_cuisine,
         |       budget            as budget,
         |       ambience,
         |       maritalStatus     as mar_sta,
         |       address,
         |       "$currJulianDate" as feed_key
         |from $userDetailsInputView
         |""".stripMargin

    // write user details data to user_details table. If table exists then append else create table
    if (SparkUtil.sparkSession.catalog.tableExists(userDetailsTable)) {
      SparkUtil.appendTable(userDetails, userDetailsTable)
    }
    else {
      SparkUtil.overwriteTable(userDetails, userDetailsTable)
    }

    // read place interaction details from userDetails.json file
    val placeInteractionDetailsQuery =
      f"""
         |select userId            as user_id,
         |       array.placeId     as place_id,
         |       array.visitDate   as visit_tm,
         |       array.rating      as user_rating,
         |       array.saleAmount  as sale_amt,
         |       "$currJulianDate" as feed_key
         |from $userDetailsInputView
         |         lateral view explode(placeInteractionDetails) a as array
         |""".stripMargin

    SparkUtil.createOrReplaceTempView(placeInteractionDetailsQuery, placeInteractionDetailsView)

    // eliminate duplicate entries if any in current feed file for place interaction details
    val placeInteractionDedupQuery =
      f"""
         |select feed_key,
         |       user_id,
         |       place_id,
         |       visit_tm,
         |       user_rating,
         |       sale_amt,
         |       to_date(visit_tm, "yyyy-MM-dd") as visit_dt
         |from (
         |         select user_id,
         |                place_id,
         |                visit_tm,
         |                user_rating,
         |                sale_amt,
         |                feed_key,
         |                row_number() over (partition by user_id, place_id order by feed_key asc ) as row_num
         |         from $placeInteractionDetailsView
         |         having row_num = 1
         |     ) as a
         |""".stripMargin

    // repartition data on user_id and place_id variables and save to table
    val placeDetailsRepartitionColumns = new ListBuffer[Column]()
    placeDetailsRepartitionColumns += new Column("user_id")
    placeDetailsRepartitionColumns += new Column("place_id")

    // check if table exist, then append data into table else overwrite partition table
    if (SparkUtil.sparkSession.catalog.tableExists(placeInteractionDetailsTable)) {
      SparkUtil.appendTable(placeInteractionDedupQuery, placeInteractionDetailsTable)
    }
    else {
      SparkUtil.overwritePartitionTable(placeInteractionDedupQuery, "visit_dt", placeInteractionDetailsTable, placeDetailsRepartitionColumns: _*)
    }

    // extract partition information of updated records. this will be used in next job to identify and process data
    val placeInteractionsUpdatedPartitionsQuery =
      f"""
         |select distinct cast(to_date(visit_tm, "yyyy-MM-dd") as string) as visit_dt
         |from (
         |         select distinct visit_tm
         |         from $placeInteractionDetailsView
         |     ) as a
         |""".stripMargin
    // write updated partition data to table
    SparkUtil.overwriteTable(placeInteractionsUpdatedPartitionsQuery, placeInteractionPartitionsTable)

  }

}
