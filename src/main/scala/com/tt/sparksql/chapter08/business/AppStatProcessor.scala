package com.tt.sparksql.chapter08.business

import com.tt.sparksql.chapter08.utils._
import com.tt.sparksql.chapter08.`trait`.DataProcess
import org.apache.spark.sql.{ SparkSession, DataFrame }

object AppStatProcessor extends DataProcess {

  override def process(spark: SparkSession) = {
    val sourceTableName = DateUtils.getTableName("ods", spark)
    val masterAddresses = "centos"

    val odsDF: DataFrame = spark.read.format("org.apache.kudu.spark.kudu")
                            .option("kudu.table", sourceTableName)
                            .option("kudu.master", masterAddresses)
                            .load()
    odsDF.createOrReplaceTempView("ods")

    val resultTmp: DataFrame = spark.sql(SQLUtils.APP_SQL_STEP1)
    resultTmp.show(false)
    resultTmp.createOrReplaceTempView("app_tmp")

    val result: DataFrame = spark.sql(SQLUtils.APP_SQL_STEP2)
    result.show(false)

    val sinkTableName = DateUtils.getTableName("app_stat", spark)
    val partitionId = "appid"
    KuduUtils.sink(result, sinkTableName, masterAddresses, SchemaUtils.APPSchema, partitionId)
  }

}
