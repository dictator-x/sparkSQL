package com.tt.sparksql.chapter04;

import org.apache.spark.sql.{SQLContext, DataFrame}
import org.apache.spark.{SparkContext, SparkConf}

object SQLContextApp {

  def main(args: Array[String]) = {
    val sparkConf: SparkConf = new SparkConf().setAppName("SQLContextAPP").setMaster("local")
    val sparkContext = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sparkContext)

    val df: DataFrame = sqlContext.read.text("file:///home/dictator/work_space/sparkSQL/data/input.txt")

    df.show()

    sparkContext.stop()
  }
}
