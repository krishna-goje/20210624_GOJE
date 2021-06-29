package com.playstation.test.batch.controller

import com.playstation.test.batch.driver.Driver
import com.playstation.test.batch.ingestion.{PlaceDetails, UserDetails}
import com.playstation.test.batch.model.AppProperties
import com.playstation.test.batch.question.Queries
import com.playstation.test.batch.transform.{RestaurantInfo, RestaurantInteraction, UserDetailsTransform}
import com.playstation.test.batch.utils.SparkUtil
import org.apache.log4j.{Level, Logger}

object EceiJobController {

  /**
   * Below method is controller for invoking specific job set via spark.app.name property
   */
  def jobController(): Unit = {
    var log: Logger = Logger.getLogger(getClass.getName.stripSuffix("$"))
    log.setLevel(Level.DEBUG)
    var sparkSession = SparkUtil.sparkSession
    val jobName = AppProperties.getApplicationName()

    sparkSession.conf

    jobName match {

      // this job will execute all jobs in pipeline
      case "run_pipeline" =>

        UserDetails.processUserDetailsData()
        PlaceDetails.processPlaceDetailsFile()
        UserDetailsTransform.processUserData()
        RestaurantInfo.restaurantInfoTransform()
        RestaurantInteraction.processRestaurantInteraction
        Queries.top3RestaurantsForCuisine()
        Queries.topNRestaurant()
        Queries.avgVisitTime()

      // this job will read userDetails.json file
      case "user_details_ingestion" =>
        UserDetails.processUserDetailsData()

      // this job will read placeDetails.json file
      case "place_details_ingestion" =>
        PlaceDetails.processPlaceDetailsFile()

      // this job will create final table with user information
      case "user_details_transform" =>
        com.playstation.test.batch.transform.UserDetailsTransform.processUserData()

      // this job will create final table with restaurant information
      case "restaurant_info_transform" =>
        com.playstation.test.batch.transform.RestaurantInfo.restaurantInfoTransform()

      // this job will create final table with user & restaurant interaction information
      case "restaurant_intr_transform" =>
        com.playstation.test.batch.transform.RestaurantInteraction.processRestaurantInteraction

      // this job will run question 1
      case "question_1" =>
        Queries.top3RestaurantsForCuisine()

      // this job will run question 2
      case "question_2" =>
        Queries.topNRestaurant()
      // this job will run question 3
      case "question_3" =>
        Queries.avgVisitTime()
      // exit job incase given job name not found
      case _ =>
        val errorMessage = s"Given job name ${jobName} is not found.Exiting the job"
        log.error(errorMessage)
        Driver.afterAll(1)
    }
  }

}
