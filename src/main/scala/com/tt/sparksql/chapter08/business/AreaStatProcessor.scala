package com.tt.sparksql.chapter08.business

import com.tt.sparksql.chapter08.`trait`.DataProcess
import com.tt.sparksql.chapter08.utils._
import org.apache.spark.sql.{ SparkSession, DataFrame }

object AreaStatProcessor extends DataProcess {

  override def process(spark: SparkSession) = {
    val sourceTableName = DateUtils.getTableName("ods", spark)
    val masterAddresses = "centos"

    val odsDF: DataFrame = spark.read.format("org.apache.kudu.spark.kudu")
                            .option("kudu.table", sourceTableName)
                            .option("kudu.master", masterAddresses)
                            .load()

    odsDF.createOrReplaceTempView("ods")

    val resultTmp: DataFrame = spark.sql(SQLUtils.AREA_SQL_STEP1)
    resultTmp.createOrReplaceTempView("area_tmp")

    val result: DataFrame = spark.sql(SQLUtils.AREA_SQL_STEP2)
    result.show

    val sinkTableName = DateUtils.getTableName("area_stat", spark)
    val partitionId = "provincename"
    // result.printSchema

    KuduUtils.sink(result, sinkTableName, masterAddresses, SchemaUtils.AREASchema, partitionId)
    // result.count
    // println(">>>>")
    // println(result.count)
    // println("============")
  }

}
