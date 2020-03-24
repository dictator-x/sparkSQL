package com.tt.sparksql.chapter08

import com.tt.sparksql.chapter08.business._
import org.apache.spark.sql.SparkSession

object SparkApp extends App {
    val spark: SparkSession = SparkSession.builder()
                                .master("local[2]")
                                .appName("ProvinceCityStatApp")
                                .getOrCreate()

    LogETLProcessor.process(spark)
    ProvinceCityStatProcessor.process(spark)
    spark.stop()
}
