package com.tt.sparksql.chapter08.business

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ SparkSession, DataFrame, SaveMode }
import com.tt.sparksql.chapter08.utils._

object LogETLApp {

  def main(args: Array[String]) = {
    val spark: SparkSession = SparkSession
                                .builder()
                                .master("local[2]")
                                .appName("LogETLApp")
                                .getOrCreate()
    import spark.implicits._

    var jsonDF: DataFrame = spark.read.json("file:///home/dictator/work_space/sparkSQL/data/data-test.json")
    // jsonDF.printSchema()
    // jsonDF.show(false)

    val ipRawRdd: RDD[String] = spark.sparkContext.textFile("file:///home/dictator/work_space/sparkSQL/data/ip.txt")

      val ipRuleDF: DataFrame = ipRawRdd.map(x => {
        val splits: Array[String] = x.split("\\|")
        val startIp: Long = splits(2).toLong
        val endIp: Long = splits(3).toLong
        val province: String = splits(6)
        val city: String = splits(7)
        val isp: String = splits(9)
        (startIp, endIp, province, city, isp)
      }).toDF("start_ip", "end_ip", "province", "city", "isp")

      import org.apache.spark.sql.functions._

      def getLongIp() = udf((ip: String) => {IPUtils.ip2Long(ip)})

      jsonDF = jsonDF.withColumn("ip_long", getLongIp()($"ip"))

      //eg: spark join
      jsonDF.join(ipRuleDF, jsonDF("ip_long").between(ipRuleDF("start_ip"), ipRuleDF("end_ip"))).show

    var jsonDF1: DataFrame = spark.read.json("file:///home/dictator/work_space/sparkSQL/data/data-test.json")

    jsonDF.createOrReplaceTempView("logs")
    ipRuleDF.createOrReplaceTempView("ips")

    val sql = SQLUtils.SQL
    // spark.sql(sql).show(false)
    val result: DataFrame = spark.sql(sql)

    val tableName = DateUtils.getTableName("ods", spark)
    val masterAddresses = "centos"
    val partitionId = "ip"

    KuduUtils.sink(result, tableName, masterAddresses, SchemaUtils.ODSSchema, partitionId)

    spark.stop()
  }
}
