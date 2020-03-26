package com.tt.sparksql.chapter07

import org.apache.kudu.client._
import org.apache.kudu.{ ColumnSchema, Schema, Type }
import java.util.{LinkedList}

object KuduAPIApp {
  def main(args: Array[String]): Unit = {

    val KUDU_MASTER = "centos"
    val client: KuduClient = new KuduClient
                                  .KuduClientBuilder(KUDU_MASTER)
                                  .build()

    val tableName = "province_city_stat"
    val newTableName = "newhelloworld"
    // createTable(client, tableName)
    // insertRows(client, tableName)
    // deleteTable(client, tableName)
    deleteTable(client, "app_stat")
    deleteTable(client, "area_stat")
    deleteTable(client, "ods")
    deleteTable(client, "province_city_stat")
    // queryTable(client, tableName)
    // alterRow(client, tableName)
    // renameTable(client, tableName, newTableName)
    client.close()
  }

  def createTable(client: KuduClient, tableName: String) = {

    import scala.collection.JavaConverters._

    val columns = List(
      new ColumnSchema.ColumnSchemaBuilder("word", Type.STRING).key(true).build(),
      new ColumnSchema.ColumnSchemaBuilder("cnt", Type.INT32).build()
    ).asJava

    val schema = new Schema(columns)
    val options: CreateTableOptions = new CreateTableOptions()
    options.setNumReplicas(1)

    val parcols: LinkedList[String] = new LinkedList[String]()
    parcols.add("word")
    options.addHashPartitions(parcols, 3)

    client.createTable(tableName, schema, options)
  }

  def deleteTable(client: KuduClient, tableName: String) = {
    client.deleteTable(tableName)
  }

  def insertRows(client: KuduClient, tableName: String) = {
    val table: KuduTable = client.openTable(tableName)
    val session: KuduSession = client.newSession()

    for ( i <- 1 to 10 ) {
      val insert: Insert = table.newInsert()
      val row: PartialRow = insert.getRow
      row.addString("word", s"pk-$i")
      row.addInt("cnt", 100+i)

      session.apply(insert)
    }
  }

  def queryTable(client: KuduClient, tableName: String) = {
    val table: KuduTable = client.openTable(tableName)
    val scanner: KuduScanner = client.newScannerBuilder(table).build()

    while ( scanner.hasMoreRows ) {
      val iterator: RowResultIterator = scanner.nextRows()

      while ( iterator.hasNext ) {
        val result: RowResult = iterator.next
        // println(result.getString("word") + "=>" + result.getInt("cnt"))
        println(result)
      }
    }
  }

  def alterRow(client: KuduClient, tableName: String) = {
    val table: KuduTable = client.openTable(tableName)
    val session: KuduSession = client.newSession()

    val update: Update = table.newUpdate()
    val row: PartialRow = update.getRow
    row.addString("word", "pk-10")
    row.addInt("cnt", 8888)
    session.apply(update)
  }

  def renameTable(client: KuduClient, tableName: String, newTableName: String) = {

    val alterOptions: AlterTableOptions = new AlterTableOptions()
    alterOptions.renameTable(newTableName)
    client.alterTable(tableName, alterOptions)
  }
}
