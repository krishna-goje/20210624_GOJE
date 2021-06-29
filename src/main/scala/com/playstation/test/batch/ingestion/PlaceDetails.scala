package com.playstation.test.batch.ingestion

import com.playstation.test.batch.model.AppProperties
import com.playstation.test.batch.utils.{DateUtil, SparkUtil}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.Column
import org.apache.spark.sql.types.{ArrayType, StringType, StructType}

import scala.collection.mutable.ListBuffer

/**
 * This object provides methods to read placeDetails json file and interpret data
 */
object PlaceDetails {

  val placeDetailsJsonFile = AppProperties.getPlaceInfoJsonFile()
  log.setLevel(Level.DEBUG)
  val inputDb = "kgoje_in"
  val placeDetailsTable = s"$inputDb.place_details"
  var log: Logger = Logger.getLogger(getClass.getName.stripSuffix("$"))

  /**
   * Below method will run every time a new json file is ingested for userDetails
   */
  def processPlaceDetailsFile(): Unit = {

    val placeDetailsInputView = "place_details"

    // get julian date for current date
    val currJulianDate = DateUtil.getCurrentDate("YYYYDDD")

    // define schema for reading userDetails.json input file
    val placeDetailsSchema = new StructType()
      .add("placeId", StringType, true)
      .add("acceptedPayments", ArrayType(StringType), true)
      .add("openHours", StringType, true)
      .add("parkingType", StringType, true)
      .add("servedCuisines", ArrayType(StringType), true)

    // invoke readJson wrapper method to read json file and create view
    SparkUtil.readJson(placeDetailsJsonFile, placeDetailsSchema, true, placeDetailsInputView)

    // Rename columns and write to table
    val placeDetailsQuery =
      f"""
         |select placeId           as place_id,
         |       acceptedPayments  as accepted_pymt,
         |       servedCuisines    as served_cuisine,
         |       openHours         as open_hrs,
         |       parkingType       as parking_typ,
         |       "$currJulianDate" as feed_key
         |from $placeDetailsInputView
         |""".stripMargin

    // repartition data on place_id to shuffle data evenly
    val placeDetailsRepartitionColumns = new ListBuffer[Column]()
    placeDetailsRepartitionColumns += new Column("place_id")

    // check if table is present. Incase table exists- append data to table else create table
    if (SparkUtil.sparkSession.catalog.tableExists(placeDetailsTable)) {
      SparkUtil.appendTable(placeDetailsQuery, placeDetailsTable, placeDetailsRepartitionColumns: _*)
    }
    else {
      SparkUtil.overwriteTable(placeDetailsQuery, placeDetailsTable, placeDetailsRepartitionColumns: _*)
    }
  }
}
