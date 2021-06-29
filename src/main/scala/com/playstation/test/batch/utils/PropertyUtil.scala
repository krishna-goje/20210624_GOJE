package com.playstation.test.batch.utils

import java.util.Properties

import com.playstation.test.batch.driver.Driver
import com.playstation.test.batch.model.AppProperties
import org.apache.log4j.{Level, Logger}

import scala.io.Source


/**
 * This object provides methods to read property files
 * Author: Krishna Goje
 */
object PropertyUtil {


  val log: Logger = Logger.getLogger(getClass.getName)
  log.setLevel(Level.DEBUG)

  /**
   * Functionality: This method will read given property file and loads into java.util.Property Object
   * Arguments:
   * 1.  fileName - String: This method will take fileName as parameter
   * Return Type: java.util.Properties Object
   */
  def readPropertyFile(fileName: String): Properties = {
    log.info("in propertyFileReader method")
    var properties: Properties = null
    val url = getClass.getClassLoader.getResource(fileName)

    try {
      val source = Source.fromInputStream(getClass().getClassLoader().getResourceAsStream(fileName))
      log.info("source: " + source)
      println("source: " + source)
      if (source != null) {
        val source = Source.fromURL(url)
        properties = new Properties()
        properties.load(source.bufferedReader())
        setProperties(properties)
      }
    }
    catch {
      case exception: Exception => {
        log.error(s"Problem reading File $fileName ")
        log.error(exception.printStackTrace())
        Driver.afterAll(1)
      }
    }
    properties
  }

  /**
   *
   * Functionality: This method will initialize properties read from property file.
   * Arguments:
   * 1.  properties - Properties: This method will take Properties object as parameter
   */
  def setProperties(properties: Properties): Unit = {

    log.info("in setProperties method")
    val defaultPropertyVal = ""

    AppProperties.setInputDb(properties.getProperty("inputDb"))
    AppProperties.setOutputDb(properties.getProperty("outputDb"))
    AppProperties.setTempDb(properties.getProperty("tempDb"))
    AppProperties.setPlaceInfoJsonFile(properties.getProperty("placeDetailsFile"))
    AppProperties.setUserDetailsJsonFile(properties.getProperty("userDetailsFile"))

    AppProperties.print
    log.info("exiting setProperties method")
  }

}
