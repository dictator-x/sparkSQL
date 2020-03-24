package com.tt.sparksql.chapter08.utils

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ SparkSession, DataFrame, SaveMode }
import org.apache.kudu.{ ColumnSchema, Schema, Type }
import org.apache.kudu.client._
import com.tt.sparksql.chapter08.utils._
import java.util.{LinkedList}

object KuduUtils {

  def sink(
    data: DataFrame,
    tableName: String,
    master: String,
    schema: Schema,
    partitionId: String
  ) = {

    val client: KuduClient = new KuduClient.KuduClientBuilder(master).build()

    if ( client.tableExists(tableName) ) {
      client.deleteTable(tableName)
    }

    val options: CreateTableOptions = new CreateTableOptions()
    options.setNumReplicas(1)

    val parcols: LinkedList[String] = new LinkedList[String]()
    parcols.add(partitionId)
    options.addHashPartitions(parcols, 3)

    client.createTable(tableName, schema, options)

    data.write.mode(SaveMode.Append)
      .format("org.apache.kudu.spark.kudu")
      .option("kudu.table", tableName)
      .option("kudu.master", master)
      .save()

  }

}
