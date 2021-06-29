package com.playstation.test.batch.utils

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.log4j.{Level, Logger}

/**
 * This object provides necessary methods for manipulating dates
 * Author: Krishna Goje
 */
object DateUtil {

  var log: Logger = Logger.getLogger(getClass.getName.stripSuffix("$"))
  log.setLevel(Level.DEBUG)

  /**
   * This method will return the current date
   * Required Arguments : Date format(ex:yyyy-MM-dd)
   */
  def getCurrentDate(dateFormat: String): String = {

    var format = new SimpleDateFormat(dateFormat)
    var RUN_MONTH_START_DT = format.format(Calendar.getInstance().getTime())
    return RUN_MONTH_START_DT
  }
}
