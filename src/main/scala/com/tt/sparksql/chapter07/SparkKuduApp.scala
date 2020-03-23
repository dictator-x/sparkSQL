package com.tt.sparksql.chapter07

import java.util.Properties
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{ DataFrame, SaveMode, SparkSession }

object SparkKuduApp {
  def main(args: Array[String]) = {
    val spark: SparkSession = SparkSession.builder()
                                .master("local")
                                .getOrCreate()

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

    val kuduMasters = "centos"
    // jdbcDF.write.mode(SaveMode.Append).format("org.apache.kudu.spark.kudu")
    //   .option("kudu.master", kuduMasters)
    //   .option("kudu.table", "tbl")
    //   .save()
    // wite to db
    // jdbcDF.write().jdbc(url, "s")
    spark.read.format("org.apache.kudu.spark.kudu")
      .option("kudu.master", kuduMasters)
      .option("kudu.table", "newhelloworld")
      .load().show
    spark.stop
  }
}
