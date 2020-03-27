package com.tt.sparksql.chapter09

import org.apache.spark.sql.{ SparkSession }
import org.apache.spark.rdd.RDD
import org.apache.spark.broadcast.Broadcast

object JoinApp02 extends App {
    val spark = SparkSession.builder().master("local").appName("JoinApp01").getOrCreate()

    val peopleInfo: collection.Map[String, String] = spark.sparkContext.parallelize(Array(("100", "pk"), ("101", "jepson"))).collectAsMap()

    val peopleBroadcast: Broadcast[collection.Map[String, String]] = spark.sparkContext.broadcast(peopleInfo)

    val peopleDetail: RDD[(String, (String, String, String))] = spark.sparkContext.parallelize(Array(("100", "ustc", "beijing"), ("103", "xxx", "shanghai"))).map(x => (x._1, x))

    peopleDetail.mapPartitions(x => {
        val broadcastPeople: collection.Map[String, String] = peopleBroadcast.value
        for((key, value) <- x if broadcastPeople.contains(key))
            yield (key, broadcastPeople.get(key).getOrElse(""), value._2)
    }).foreach(println)

    Thread.sleep(20000)
    spark.stop()

}
