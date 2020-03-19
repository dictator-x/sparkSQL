package com.tt.sparksql.chapter04

import org.apache.spark.sql.{SparkSession, DataFrame}

object DataFrameAPIApp {

  def main(args: Array[String]) = {
    val spark = SparkSession.builder()
                  .master("local")
                  .appName("DataFrameAPIApp")
                  .getOrCreate()

    val df: DataFrame = spark.read.json("file:///home/dictator/work_space/sparkSQL/data/people.json")

    df.printSchema()
    df.show()

    df.select("name").show()

    import spark.implicits._

    df.select($"name").show()

    df.filter($"age" > 21).show()
    df.filter("age > 21").show()

    df.groupBy("age").count().show()

    df.createOrReplaceTempView("people")

    spark.sql("select name from people where age > 21").show()

    df.select($"name", ($"age"+10).as("new_age")).show()

    spark.stop()
  }
}
