package com.tt.sparksql.chapter05;

import org.apache.spark.sql.{SparkSession, DataFrame, Dataset, SaveMode}
import com.typesafe.config.ConfigFactory

object DataSourceApp {

  def main(args: Array[String]) = {
    val spark = SparkSession.builder()
                  .master("local")
                  .appName("DataSourceApp")
                  .getOrCreate()

    // text(spark)
    // json(spark)
    // common(spark)
    // convert(spark)
    // jdbc(spark)
    jdbc2(spark)

    spark.stop()
  }

  def jdbc2(spark: SparkSession) = {
    import spark.implicits._

    val config = ConfigFactory.load()
    val driver = config.getString("db.default.driver")
    val url = config.getString("db.default.url")
    val user = config.getString("db.default.user")
    val password = config.getString("db.default.password")
    val database = config.getString("db.default.database")
    val table = config.getString("db.default.table")

    val jdbcDF = spark.read
                  .format("jdbc")
                  // Coordinate with mysql8 jdbc connection
                  .option("url", url)
                  .option("driver", driver)
                  .option("dbtable", s"$database.$table")
                  .option("user", user)
                  .option("password", password)
                  .load()
    jdbcDF.select("TBL_ID", "TBL_NAME").show()
    // wite to db
    // jdbcDF.write().jdbc(url, "s")
  }

  def jdbc(spark: SparkSession) = {
    import spark.implicits._

    val jdbcDF = spark.read
                  .format("jdbc")
                  // Coordinate with mysql8 jdbc connection
                  .option("url", "jdbc:mysql://centos:3306/hadoop_hive?serverTimezone=UTC&useSSL=false")
                  .option("driver", "com.mysql.jdbc.Driver")
                  .option("dbtable", "TBLS")
                  .option("user", "root")
                  .option("password", "rootroot")
                  .load()
    jdbcDF.select("TBL_ID", "TBL_NAME").show()

    import java.util.Properties

    val connectionProperties = new Properties()
    connectionProperties.put("user", "root")
    connectionProperties.put("password", "rootroot")
    spark.read.jdbc("jdbc:mysql://centos:3306/hadoop_hive?serverTimezone=UTC&useSSL=false", "TBLS", connectionProperties).show()

  }

  // Json -> Parquet
  def convert(spark: SparkSession) = {
    import spark.implicits._

    val jsonDF: DataFrame = spark.read.json("file:///home/dictator/work_space/sparkSQL/data/people.json")
    jsonDF.show()

    jsonDF.filter("age > 20").write.format("parquet").mode(SaveMode.Overwrite).save("out")

    spark.read.parquet("file:///home/dictator/work_space/sparkSQL/out").show()
  }

  def parquet(spark: SparkSession) = {
    import spark.implicits._

    val parquetDF: DataFrame = spark.read.parquet("file:///home/dictator/work_space/sparkSQL/data/users.parquet")

      parquetDF.printSchema()
      parquetDF.show()

      parquetDF
        .select("name", "favorite_numbers")
        .write
        .mode("overwrite")
        .option("compression", "none")
        .parquet("out")

      spark.read.parquet("file:///home/dictator/work_space/sparkSQL/out").show()
  }

  def common(spark: SparkSession) = {
    import spark.implicits._

    val textDF: DataFrame = spark.read.format("text").load("file:///home/dictator/work_space/sparkSQL/data/people.txt")
    val jsonDF: DataFrame = spark.read.format("json").load("file:///home/dictator/work_space/sparkSQL/data/people.json")

    textDF.show()
    jsonDF.show()
    jsonDF.write.format("json").mode("overwrite").save("out")
  }

  def json(spark: SparkSession) = {
    import spark.implicits._

    val jsonDF: DataFrame = spark.read.json("file:///home/dictator/work_space/sparkSQL/data/people.json")

    jsonDF.show()

    jsonDF.filter("age > 20").select("name").write.mode("overwrite").json("out")

    val jsonDF2: DataFrame = spark.read.json("file:///home/dictator/work_space/sparkSQL/data/people2.json")

    jsonDF2.show()
    jsonDF2.select($"name", $"age", $"info.work".as("work"), $"info.home".as("home")).write.mode(SaveMode.Overwrite).json("out")

  }

  def text(spark: SparkSession) = {
    import spark.implicits._

    val textDF: DataFrame = spark.read.text("file:///home/dictator/work_space/sparkSQL/data/people.txt")
      textDF.show()

    // By default text source can only suport one column
    val result: Dataset[(String)] = textDF.map(x =>{
      val splits: Array[String] = x.getString(0).split(",")
      (splits(0).trim)//, splits(1).trim)
    })


    // result.write.mode(SaveMode.Overwrite).text("out")
    result.write.mode("overwrite").text("out")
  }
}
