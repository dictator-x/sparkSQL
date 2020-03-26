package com.tt.sparksql.chapter08.business

import com.tt.sparksql.chapter08.`trait`.DataProcess
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ SparkSession, DataFrame }
import com.tt.sparksql.chapter08.utils._

object LogETLProcessor extends DataProcess {

  override def process(spark: SparkSession) = {
    import spark.implicits._

    val rawPath: String = spark.sparkContext.getConf.get("spark.raw.path")

    var jsonDF: DataFrame = spark.read.json(rawPath)
    // jsonDF.printSchema()
    // jsonDF.select("sessionId").show(1000, false)

    val ipRulePath: String = spark.sparkContext.getConf.get("spark.ip.path")
    val ipRawRdd: RDD[String] = spark.sparkContext.textFile(ipRulePath)

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

    jsonDF.createOrReplaceTempView("logs")
    ipRuleDF.createOrReplaceTempView("ips")

    val sql = SQLUtils.SQL
    // spark.sql(sql).show(false)
    val result: DataFrame = spark.sql(sql)

    val tableName = DateUtils.getTableName("ods", spark)
    val masterAddresses = "centos"
    val partitionId = "ip"

    KuduUtils.sink(result, tableName, masterAddresses, SchemaUtils.ODSSchema, partitionId)

  }
}
