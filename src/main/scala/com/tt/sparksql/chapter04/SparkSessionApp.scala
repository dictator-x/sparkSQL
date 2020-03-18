package com.tt.sparksql.chapter04

import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkSessionApp {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
                                .builder()
                                .master("local")
                                .getOrCreate();

    val df: DataFrame = spark.read.text("file:///home/dictator/work_space/sparkSQL/data/input.txt")

    df.show()
    spark.stop()
  }

}
