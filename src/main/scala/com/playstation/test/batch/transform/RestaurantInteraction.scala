package com.playstation.test.batch.transform

import com.playstation.test.batch.model.AppProperties
import com.playstation.test.batch.utils.SparkUtil
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.Column

import scala.collection.mutable.ListBuffer

/**
 * This object will process restaurant interaction and create final table
 *
 */
object RestaurantInteraction {

  val inputDb = AppProperties.getInputDb
  log.setLevel(Level.DEBUG)
  val tempDb = AppProperties.getTempDb()
  val outputDb = AppProperties.getOutputDb()
  val restaurantIntrInfoTable = s"$outputDb.restaurant_intr_info"
  var log: Logger = Logger.getLogger(getClass.getName.stripSuffix("$"))

  /**
   * This method processes restaurant interaction information and creates final table
   */
  def processRestaurantInteraction: Unit = {

    val spark = SparkUtil.sparkSession
    import spark.implicits._

    // extract list of updated partition data
    val restaurantInteractionPartitions = SparkUtil.sparkSession.read.table(s"$tempDb.place_interaction_partitions")
      .select("visit_dt").map(f => f.getString(0)).collect().toList

    //convert partition list to string such that it can used in filters
    var partitionDaysToExtract: String = ""
    restaurantInteractionPartitions.foreach(partitionDaysToExtract += "'" + _ + "',")
    partitionDaysToExtract = partitionDaysToExtract.stripSuffix(",")

    log.info(s"scanning partitions: $partitionDaysToExtract")

    /**
     * Query will provide restaurant interaction information with cuisine type added to table
     * How it works?
     * Stage 1:
     * Read data from place_interaction_details ingestion table with partition filters on updated partitions list
     * Dedup data on user_id, place_id and visit_tm columns
     * Stage 2:
     * Join data from stage 1 with restaurant_info table and extract served_cuisine column
     */
    val query =
      f"""
         |select A.feed_key,
         |       user_id,
         |       A.place_id,
         |       B.served_cuisine as served_cuisine,
         |       visit_tm,
         |       user_rating,
         |       sale_amt,
         |       visit_dt
         |from (
         |         select feed_key,
         |                user_id,
         |                place_id,
         |                visit_tm,
         |                user_rating,
         |                sale_amt,
         |                visit_dt,
         |                row_number() over (partition by user_id,place_id,visit_tm order by visit_tm desc) as rank
         |         from $inputDb.place_interaction_details
         |         where visit_dt in ($partitionDaysToExtract)
         |         having rank=1
         |     ) as A
         |         left outer join $outputDb.restaurant_info B
         |on A.place_id=B.place_id
         |""".stripMargin

    // repartition data on user_id and served_cuisine columns.
    // This will help with optimal query reads as data is equally shuffle across various files
    val placeInteractionRepartitionColumns = new ListBuffer[Column]()
    placeInteractionRepartitionColumns += new Column("user_id")
    placeInteractionRepartitionColumns += new Column("served_cuisine")

    // check if table exists then overwrite partition else create new table
    if (SparkUtil.sparkSession.catalog.tableExists(restaurantIntrInfoTable)) {
      SparkUtil.insertOverwritePartition(query, restaurantIntrInfoTable, "visit_dt", placeInteractionRepartitionColumns: _*)
    }
    else {
      SparkUtil.overwritePartitionTable(query, "visit_dt", restaurantIntrInfoTable, placeInteractionRepartitionColumns: _*)
    }

  }

}
