package com.tt.sparksql.chapter04

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SparkSession, DataFrame}

object InteroperatingRDDApp {

  def main(args: Array[String]) = {
    val spark = SparkSession.builder()
                  .master("local")
                  .appName("InteroperatingRDDApp")
                  .getOrCreate()

    // runInferSchema(spark)

    runProgrammaticSchema(spark)

    spark.stop()
  }

  private def runProgrammaticSchema(spark: SparkSession) = {
    import spark.implicits._
    import org.apache.spark.sql.Row
    import org.apache.spark.sql.types._

    val peopleRDD: RDD[String] = spark.sparkContext.textFile("file:///home/dictator/work_space/sparkSQL/data/people.txt")

    val peopleRowRDD: RDD[Row] = peopleRDD.map(_.split(","))
      .map(x=>Row(x(0), x(1).trim.toInt))

    val struct = StructType(
      StructField("name", StringType, true) ::
      StructField("age", IntegerType, false) :: Nil
    )

    val peopleDF: DataFrame = spark.createDataFrame(peopleRowRDD, struct)

    peopleDF.show()

  }

  private def runInferSchema(spark: SparkSession) = {
    import spark.implicits._

    val peopleRDD: RDD[String] = spark.sparkContext.textFile("file:///home/dictator/work_space/sparkSQL/data/people.txt")

    val peopleDF: DataFrame = peopleRDD
      .map(_.split(","))
      .map(x => People(x(0), x(1).trim.toInt))
      .toDF
    peopleDF.show(false)

    peopleDF.createOrReplaceTempView("people")
    val queryDF: DataFrame = spark.sql("select name, age from people where age between 19 and 29")
    queryDF.show()

    //Read by index
    queryDF.map(x => "Name: " + x(0)).show()

    //Read by column name
    queryDF.map(x => "Name: " + x.getAs[String]("name")).show()


  }

  case class People(name:String, age:Int)
}
