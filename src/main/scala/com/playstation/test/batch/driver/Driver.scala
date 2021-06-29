package com.playstation.test.batch.driver

import com.playstation.test.batch.controller.EceiJobController
import com.playstation.test.batch.model.AppProperties
import com.playstation.test.batch.utils.{PropertyUtil, SparkUtil}
import org.apache.log4j.{Level, Logger}

/**
 * Author: Krishna Goje
 */
object Driver {
  var log: Logger = Logger.getLogger(getClass.getName.stripSuffix("$"))
  log.setLevel(Level.DEBUG)

  var afterAllCall: Boolean = false

  /**
   * Main method. This is driver for invoking all jobs. 1st entry point for application
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {
    try {
      log.debug("in main method")
      init()
      EceiJobController.jobController()
      afterAll(0)
    }
    catch {
      case exception: Exception => {
        exception.printStackTrace()
      }
    }
  }

  /**
   * Description: This method will run after all the jobs have completed.
   * possible values are 0 or 1 for exitCode.
   * 0-> Represents Successful execution
   * 1-> Failure in flow
   */
  def afterAll(exitCode: Int): Unit = {
    log.debug("in afterAll method")
    if (!afterAllCall) {
      afterAllCall = true
      log.info("###################################################################################")
      log.info("CLOSING SPARK SESSION")
      SparkUtil.sparkSession.close()
      log.info("SPARK SESSION CLOSED")
      log.info("###################################################################################")
      log.info(s"EXITING PROGRAM WITH EXIT CODE: ${exitCode}")
      sys.exit(exitCode)
    }
    else {
      log.error("Exiting the program.")
      sys.exit(exitCode)
    }
  }

  /**
   * Functionality: This method will initialize all necessary job parameters.
   */
  def init() {

    log.debug("in init method ")
    /* Set start time for Job */

    /* create spark session */
    log.debug("creating spark session ")
    // create spark session object
    SparkUtil.getSparkSession()
    // read common.properties file for necessary properties
    PropertyUtil.readPropertyFile("common.properties")
    // read properties set via spark application
    AppProperties.collectAppConf()

    log.debug("collecting application configs ")
  }
}
