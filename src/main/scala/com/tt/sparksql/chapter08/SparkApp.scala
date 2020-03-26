package com.tt.sparksql.chapter08

import com.tt.sparksql.chapter08.business._
import org.apache.spark.sql.SparkSession
import org.apache.commons.lang3.StringUtils
import org.apache.spark.internal.Logging

object SparkApp extends Logging {

  def main(args: Array[String]) = {
    val spark: SparkSession = SparkSession.builder()
                                // .master("local[2]")
                                // .appName("SparkApp")
                                .getOrCreate()

    // set time.
    val time = spark.sparkContext.getConf.get("spark.time")
    if ( StringUtils.isBlank(time) ) {
      logError("process time should not be empty")
      System.exit(0)
    }

    LogETLProcessor.process(spark)
    ProvinceCityStatProcessor.process(spark)
    AreaStatProcessor.process(spark)
    AppStatProcessor.process(spark)
    spark.stop()
  }
}
