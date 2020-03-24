package  com.tt.sparksql.chapter08.business

import com.tt.sparksql.chapter08.`trait`.DataProcess
import com.tt.sparksql.chapter08.utils._
import org.apache.spark.sql.{ SparkSession, DataFrame, SaveMode }

object ProvinceCityStatProcessor extends DataProcess {

  override def process(spark: SparkSession) = {

    val sourceTableName = "ods"
    val masterAddresses = "centos"

    val odsDF: DataFrame = spark.read.format("org.apache.kudu.spark.kudu")
                            .option("kudu.table", sourceTableName)
                            .option("kudu.master", masterAddresses)
                            .load()
    // odsDF.show()

    odsDF.createOrReplaceTempView("ods")
    val result: DataFrame = spark.sql(SQLUtils.PROVINCE_CITY_SQL)
    result.show(false)

    val tableName = "province_city_stat"
    val partitionId = "provincename"

    KuduUtils.sink(result, tableName, masterAddresses, SchemaUtils.ProvinceCitySchema, partitionId)
  }
}
