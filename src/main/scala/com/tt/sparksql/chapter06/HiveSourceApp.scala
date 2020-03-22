package com.tt.sparksql.chapter06

import org.apache.spark.sql.{ SparkSession, DataFrame }
import com.typesafe.config.ConfigFactory
import java.util.Properties

object HiveSourceApp {

  def main(args: Array[String]) = {
    val spark: SparkSession = SparkSession.builder().master("local").appName("HiveSourceApp")
      //TODO: check why this line cause create hive table fail
      // .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .enableHiveSupport()
      .getOrCreate()

    val config = ConfigFactory.load()
    val driver = config.getString("db.default.driver")
    val url = config.getString("db.default.url")
    val user = config.getString("db.default.user")
    val password = config.getString("db.default.password")
    val database = config.getString("db.default.database")
    val table = config.getString("db.default.table")

    val connectionProperties = new Properties()
    connectionProperties.put("user", user)
    connectionProperties.put("password", password)

    val jdbcDF: DataFrame = spark.read
      .jdbc(url, s"$database.$table", connectionProperties)

    jdbcDF.show

    // spark.catalog.listDatabases.show(100, false)
    // In order to let sparkSesson able to access hive. copy hive-site.xml
    // under resources.
    spark.table("helloworld").show

    jdbcDF.write.saveAsTable("tbls")
    spark.stop()
  }
}
