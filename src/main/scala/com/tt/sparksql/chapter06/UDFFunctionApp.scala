package com.tt.sparksql.chapter06

import org.apache.spark.rdd.RDD

import org.apache.spark.sql.{ SparkSession, DataFrame }

object UDFFunctionApp {
  def main(args: Array[String]) = {

    val spark: SparkSession = SparkSession.builder()
      .master("local").appName("HiveSourceApp")
      .getOrCreate()

    import spark.implicits._

    val infoRDD: RDD[String] = spark.sparkContext.textFile("file:///home/dictator/work_space/sparkSQL/data/hobbies.txt")
    val infoDF: DataFrame = infoRDD.map(_.split(" ")).map(x => {
        Hobbies(x(0), x(1))
      }).toDF

    infoDF.show

    spark.udf.register("hobby_num", (s: String) => s.split(",").size)
    infoDF.createOrReplaceTempView("hobbies")
    spark.sql("select name, hobbies, hobby_num(hobbies) from hobbies").show(false)

    spark.stop()
  }

  case class Hobbies(name: String, hobbies: String)
}
