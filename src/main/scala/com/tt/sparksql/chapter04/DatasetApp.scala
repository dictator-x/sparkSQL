package com.tt.sparksql.chapter04

import org.apache.spark.sql.{SparkSession, Dataset, DataFrame}

object DatasetApp {
  def main(args: Array[String]) = {
    val spark = SparkSession.builder()
                  .master("local")
                  .appName("DataFrameAPIApp")
                  .getOrCreate()
    import spark.implicits._

    val ds: Dataset[Person] = Seq(Person("PK", "20")).toDS()
    ds.show()

    val primitiveDS: Dataset[Int] = Seq(1,2,4).toDS
    primitiveDS.map(x => x+1).collect().foreach(println)

    val peopleDF: DataFrame = spark.read.json("file:///home/dictator/work_space/sparkSQL/data/people.json")
      val peopleDS: Dataset[Person] = peopleDF.as[Person]

    peopleDF.select("name").show()

    peopleDS.map(x => x.name).show()

    spark.stop()
  }

  case class Person(name: String, age: String)
}
