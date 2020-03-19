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

    val zips: DataFrame = spark.read.json("file:///home/dictator/work_space/sparkSQL/data/zips.json")

    zips.printSchema()
    zips.show()
    zips.groupBy("city").count().show()
    zips.show(5, false)

    zips.head(3).foreach(println)

    zips.take(5).foreach(println)

    val count: Long = zips.count()
    println(s"Total Counts: $count")

    zips.select($"_id" as "id", $"city").filter($"pop" > 40000).show(10, false)
    zips.filter($"pop" > 40000).withColumnRenamed("_id", "new_id").show(10, false)

    import org.apache.spark.sql.functions._

    zips.select("_id", "city", "pop", "state").filter(zips.col("state") === "CA").orderBy(desc("pop")).show(false)

    zips.createOrReplaceTempView("zip")
    spark.sql("select _id, city, pop, state from zip where state='CA' order by pop desc limit 10").show()

    spark.stop()
  }
}
