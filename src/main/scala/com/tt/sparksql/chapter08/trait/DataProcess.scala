package com.tt.sparksql.chapter08.`trait`

import org.apache.spark.sql.SparkSession

trait DataProcess {
  def process(spark: SparkSession)
}
