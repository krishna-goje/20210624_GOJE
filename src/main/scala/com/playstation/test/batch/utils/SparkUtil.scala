package com.playstation.test.batch.utils

import com.playstation.test.batch.driver.Driver
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, SaveMode, SparkSession}

/**
 * This object provides wrapper methods to interact with spark
 * Author: Krishna Goje
 */
object SparkUtil {

  // set file format to orc
  val fileStorageFormat = "orc"
  log.setLevel(Level.DEBUG)
  /**
   * Use one of the following compression codec
   * 1.  org.apache.spark.io.LZ4CompressionCodec
   * 2.  org.apache.spark.io.LZFCompressionCodec
   * 3.  org.apache.spark.io.SnappyCompressionCodec
   * 4.  org.apache.spark.io.ZstdCompressionCodec
   */
  val fileCompression = "snappy"
  val retryWait = (1000 * 120) // 120 seconds wait. converted to nanoseconds(1000*120)
  var log: Logger = Logger.getLogger(getClass.getName.stripSuffix("$"))
  var sparkSession: SparkSession = _
  var retryCount: Int = 3

  /**
   * Functionality: This method will create sparksession object. It will retry 3 times in case session is not created
   * */

  def getSparkSession(): SparkSession = {
    log.debug("######################################################################")
    log.debug(s"in getSparkSession method. retry count: $retryCount ")

    if (sparkSession != null) return sparkSession
    if (retryCount <= 0) {
      log.error("Unable to create spark session object after 3 attempts. Exiting the Flow")
      Driver.afterAll(1)
    }
    try {
      sparkSession = SparkSession
        .builder()
        .enableHiveSupport()
        .getOrCreate()

      // suppress debug logs from spark
      Logger.getLogger("org").setLevel(Level.OFF)
      Logger.getLogger("akka").setLevel(Level.OFF)

      // incase sparkSession is not created. retry again (total of 3 attempts)
      if (sparkSession == null) {
        log.error("Unable to create sparkSession object")
        log.info("retrying for spark session.")
        log.info(s"Retry Count: $retryCount")
        log.info(s"sleeping for : $retryWait milli seconds before retrying")
        // decrement retry count by 1
        retryCount = (retryCount - 1)
        Thread.sleep(retryWait)
        getSparkSession()
      }
    }
    catch {
      case exception: Exception =>
        log.error("Exception in creating spark session")
        log.error(exception.printStackTrace())
        log.error("Unable to create sparkSession object")

        log.info("retrying for spark session.")
        log.info(s"retry Count: $retryCount")
        log.info(s"sleeping for $retryWait milli seconds before retrying")
        // decrement retry count by 1
        retryCount = (retryCount - 1)
        Thread.sleep(retryWait)
        getSparkSession()
        Driver.afterAll(1)
    }
    log.debug("Exiting getSparkSession method")
    sparkSession
  }

  /**
   * this method acts as wrapper method to overwrite table. Takes below parameters
   *
   * @param query     -> SQL Query to run
   * @param tableName -> Table to create
   * @param columns   -> data to be shuffled based on any columns
   */
  def overwriteTable(query: String, tableName: String, columns: Column*): Unit = {
    try {
      log.debug("######################################################################")
      log.debug("in overwriteTable method")
      log.info("OVERWRITING TABLE: " + tableName)
      log.info("EXECUTING QUERY : " + query)

      var dataFrame = sparkSession.sql(query)
      dataFrame = dataFrame.repartition(columns: _*)
      log.info("############################################################################################")
      log.info("QUERY LOGICAL AND PHYSICAL PLAN")
      log.info(dataFrame.explain)

      dataFrame.write.format(fileStorageFormat)
        .option("compression", fileCompression)
        .option("orc.create.index", "true")
        .option("orc.schema.evolution.case.sensitive", "false")
        .option("orc.bloom.filter.columns", "*")
        .mode(SaveMode.Overwrite).saveAsTable(tableName)

    } catch {
      case exception: Exception =>
        log.error(s"Exception in $tableName sql query")
        log.error(exception.printStackTrace())
        Driver.afterAll(1)
    }
  }

  /**
   * This method is wrapper to read json file and create view
   *
   * @param inputFile  -> input file to read
   * @param jsonSchema -> schema to interpret input data
   * @param multiline  -> if input file has json object in multiple lines
   * @param viewName   -> view to create
   */
  def readJson(inputFile: String, jsonSchema: StructType, multiline: Boolean = true, viewName: String): Unit = {
    try {
      val inputData = sparkSession.read.schema(jsonSchema)
        .option("multiline", multiline)
        .json(inputFile)
      inputData.createOrReplaceTempView(viewName)
      log.info("view created successfully")
    }
    catch {
      case exception: Exception =>
        val errorMsg = s"Exception in reading json file: $inputFile"
        log.error(exception.printStackTrace())
        log.error(errorMsg)
        Driver.afterAll(1)
    }
  }

  /**
   * This method is wrapper for creating view in spark. Use cache param if dataframe needs to cached.
   *
   * @param query    -> SQL query to run
   * @param viewName -> view to create
   * @param cache    -> Set this param to true, incase view needs to be cached to default storage level (MEMORY_AND_DISK)
   */
  def createOrReplaceTempView(query: String, viewName: String, cache: Boolean = false): Unit = {
    log.debug("######################################################################")
    try {
      log.debug("in createOrReplaceTempView method")
      log.debug(s"Creating a GlobalTempView $viewName with query string")
      log.info(s"VIEW NAME: $viewName")
      log.info("QUERY: " + query)
      val sql = sparkSession.sql(query)
      sql.createOrReplaceTempView(viewName)

      if (cache == true) {
        sparkSession.catalog.cacheTable(viewName)
      }
      log.info(sql.explain)
    } catch {
      case exception: Exception => {
        log.error(s"Exception in $query sql query")
        log.error(exception.printStackTrace())
        Driver.afterAll(1)
      }
    }
  }

  /**
   * This method acts as wrapper to append data into existing table.
   *
   * @param query     -> SQL Query to run
   * @param tableName -> table to update
   * @param columns   -> columns list in case data to be shuffled
   */
  def appendTable(query: String, tableName: String, columns: Column*): Unit = {
    try {
      log.debug("######################################################################")
      log.debug("in appendTable method")
      log.info("APPENDING TABLE: " + tableName)
      log.info("EXECUTING QUERY : " + query)

      sparkSession.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
      sparkSession.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")

      var dataFrame = sparkSession.sql(query)

      log.info("############################################################################################")
      log.info(dataFrame.explain)
      log.info("############################################################################################")
      dataFrame.write.format(fileStorageFormat)
        .option("compression", fileCompression)
        .option("orc.create.index", "true")
        .option("orc.schema.evolution.case.sensitive", "false")
        .option("orc.bloom.filter.columns", "*")
        .mode(SaveMode.Append).insertInto(tableName)

    } catch {
      case exception: Exception =>
        log.error(s"Exception in $tableName sql query")
        log.error(exception.printStackTrace())
        Driver.afterAll(1)
    }
  }

  /**
   * This method acts as wrapper for drop and create of partitioned table
   *
   * @param query       -> SQL Query to run
   * @param partColumn  -> partition column of table
   * @param targetTable -> table to create
   * @param columns     -> in case data has to be shuffled based on columns list
   */
  def overwritePartitionTable(query: String, partColumn: String, targetTable: String, columns: Column*): Unit = {

    try {
      log.info("############################################################################################")
      log.debug("IN OVERWRITE PARTITION TABLE METHOD")

      log.info(s"creating Table: $targetTable")
      log.info(s"PARTITION COLUMN : $partColumn")
      log.info(s"QUERY: $query")

      var dataFrame = sparkSession.sql(query).repartition(columns: _*)
      log.info("QUERY LOGICAL AND PHYSICAL PLAN")
      log.info(dataFrame.explain)

      dataFrame.write.partitionBy(partColumn).format(fileStorageFormat)
        .option("compression", fileCompression)
        .option("orc.create.index", "true")
        .option("orc.schema.evolution.case.sensitive", "false")
        .option("orc.bloom.filter.columns", "*")
        .mode(SaveMode.Overwrite).saveAsTable(targetTable)

      log.info(f" created $targetTable%s  table with $partColumn as partition column")

    } catch {
      case exception: Exception =>
        log.error(
          f"Exception in creating $targetTable%s  table with $partColumn as partition column "
        )
        log.error(exception.printStackTrace())
        Driver.afterAll(1)
    }

  }

  /**
   * This method will overwrite certain partition in a table
   *
   * @param query       -> SQL Query to run
   * @param targetTable -> table to update data
   * @param partColumn  -> partition column
   * @param columns     -> List of column to shuffle data
   */
  def insertOverwritePartition(query: String, targetTable: String, partColumn: String, columns: Column*): Unit = {

    try {
      log.debug("######################################################################")
      log.debug("IN OVERWRITE PARTITION METHOD")

      log.info(s"TABLE: $targetTable")
      log.info(s"PARTITION : $partColumn")
      log.info(s"QUERY : ${query}")

      var dataFrame = sparkSession.sql(query).repartition(columns: _*)

      sparkSession.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
      sparkSession.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")

      log.info(
        "############################################################################################"
      )
      log.info("QUERY LOGICAL AND PHYSICAL PLAN")
      log.info(dataFrame.explain)

      dataFrame.write.format(fileStorageFormat)
        .option("compression", fileCompression)
        .option("orc.create.index", "true")
        .option("orc.schema.evolution.case.sensitive", "false")
        .option("orc.bloom.filter.columns", "*")
        .mode(SaveMode.Overwrite).insertInto(targetTable)

      log.info(f" overwriting $partColumn in  $targetTable table")

    } catch {
      case exception: Exception =>
        log.error(f"Exception in overwriting $partColumn in  $targetTable ")
        log.error(exception.printStackTrace)
        Driver.afterAll(1)
    }
  }


}
