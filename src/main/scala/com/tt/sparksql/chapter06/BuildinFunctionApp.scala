package com.tt.sparksql.chapter06;

import org.apache.spark.sql.{ SparkSession, DataFrame }
import org.apache.spark.rdd.RDD

object BuildinFunctionApp {

  def main(args: Array[String]) = {

    val spark: SparkSession = SparkSession.builder().master("local").appName("BuildinFunctionApp")
      .getOrCreate()

    val userAccessLog = Array(
      "2016-10-01, 1122",
      "2016-10-01, 1122",
      "2016-10-01, 1123",
      "2016-10-01, 1124",
      "2016-10-02, 1124",
      "2016-10-02, 1122",
      "2016-10-02, 1121",
      "2016-10-02, 1123",
      "2016-10-02, 1123"
    )

    import spark.implicits._
    import org.apache.spark.sql.functions._

    val userAccessRDD: RDD[String] = spark.sparkContext.parallelize(userAccessLog)
    val userAccessDF: DataFrame = userAccessRDD.map(x => {
      val splits:Array[String] = x.split(",")
      Log(splits(0), splits(1).trim.toInt)
    }).toDF

    userAccessDF.show()

    userAccessDF.groupBy("day").agg(count("userId").as("pv")).show()
    userAccessDF.groupBy("day").agg(countDistinct("userId").as("uv")).show()

    spark.stop()
  }

  case class Log(day:String, userId:Int)
}
