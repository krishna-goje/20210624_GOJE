package com.playstation.test.batch.model

import com.playstation.test.batch.utils.SparkUtil
import org.apache.log4j.{Level, Logger}

/**
 *
 * This is model class to store required configurations from spark session.
 * Author: Krishna Goje
 */
object AppProperties {

  private val log: Logger = Logger.getLogger(getClass.getName)
  log.setLevel(Level.DEBUG)

  private var applicationId: String = _
  private var applicationName: String = _
  private var userDetailsJsonFile: String = _
  private var placeInfoJsonFile: String = _

  private var inputDb: String = _
  private var tempDb: String = _
  private var outputDb: String = _


  def getApplicationId(): String = {
    return applicationId
  }

  def setApplicationId(_id: String): Unit = {
    log.info(s"Application ID set to ${_id}")
    applicationId = _id
  }

  def getApplicationName(): String = {
    return applicationName
  }

  def getUserDetailsJsonFile(): String = {
    return userDetailsJsonFile
  }

  def getPlaceInfoJsonFile(): String = {
    return placeInfoJsonFile
  }

  def collectAppConf(): Unit = {
    try {
      if (SparkUtil.sparkSession.sparkContext.getConf.contains("spark.app.name")) {
        val _appName = SparkUtil.sparkSession.sparkContext.getConf.get("spark.app.name").toString
        log.info(s"spark.app.name property found : ${_appName}. SETTING APPLICATION NAME : ${_appName}")
        AppProperties.setApplicationName(_appName)
      }
      else {
        log.warn("spark.app.name PROPERTY NOT FOUND.")
      }
    }
    catch {
      case exception: Exception => {
        log.error("ERROR IN FETCHING PROPERTY spark.app.name.")
        exception.printStackTrace
      }
    }


    try {
      if (SparkUtil.sparkSession.sparkContext.getConf.contains("user.details.fileName")) {
        val _userDetailsFile = SparkUtil.sparkSession.sparkContext.getConf.get("user.details.fileName").toString
        log.info(s"user.details.fileName property found : ${_userDetailsFile}. setting user details file name : ${_userDetailsFile}")
        AppProperties.setUserDetailsJsonFile(_userDetailsFile)
      }
      else {
        log.warn("user.details.fileName PROPERTY NOT FOUND. Value from common.properties file takes precedence")
      }
    }
    catch {
      case exception: Exception => {
        log.error("ERROR IN FETCHING PROPERTY user.details.fileName")
        exception.printStackTrace
      }
    }

    try {
      if (SparkUtil.sparkSession.sparkContext.getConf.contains("place.info.fileName")) {
        val _placeInfoFile = SparkUtil.sparkSession.sparkContext.getConf.get("place.info.fileName").toString
        log.info(s"place.info.fileName property found : ${_placeInfoFile}. setting user details file name : ${_placeInfoFile}")
        AppProperties.setPlaceInfoJsonFile(_placeInfoFile)
      }
      else {
        log.warn("place.info.fileName PROPERTY NOT FOUND. Value from common.properties file takes precedence")
      }
    }
    catch {
      case exception: Exception => {
        log.error("ERROR IN FETCHING PROPERTY place.info.fileName")
        exception.printStackTrace
      }
    }

  }

  def setApplicationName(_name: String): Unit = {
    log.info(s"Application Name set to ${_name}")
    applicationName = _name
  }

  def setUserDetailsJsonFile(_fileName: String): Unit = {
    log.info(s"User Details Json File set to ${_fileName}")
    userDetailsJsonFile = _fileName
  }

  def setPlaceInfoJsonFile(_fileName: String): Unit = {
    log.info(s"User Details Json File set to ${_fileName}")
    placeInfoJsonFile = _fileName
  }

  def getInputDb(): String = {
    return inputDb
  }

  def setInputDb(_name: String): Unit = {
    log.info(s"Input Db set to ${_name}")
    inputDb = _name
  }

  def getTempDb(): String = {
    return tempDb
  }

  def setTempDb(_name: String): Unit = {
    log.info(s"Temp Db set to ${_name}")
    tempDb = _name
  }

  def getOutputDb(): String = {
    return outputDb
  }

  def setOutputDb(_name: String): Unit = {
    log.info(s"Output Db set to ${_name}")
    outputDb = _name
  }

  def print: Unit = {
    log.debug("in print method")

    log.info(s"applicationId: $applicationId")
    log.info(s"applicationName: $applicationName")
    log.info(s"userDetailsJsonFile: $userDetailsJsonFile")
    log.info(s"placeInfoJsonFile: $placeInfoJsonFile")
    log.info(s"inputDb: $inputDb")
    log.info(s"tempDb: $tempDb")
    log.info(s"outputDb: $outputDb")

  }


}

