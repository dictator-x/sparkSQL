package com.tt.sparksql.chapter02

import org.apache.spark.{SparkConf, SparkContext}

object SparkWordCountApp {
    def main(args: Array[String]): Unit = {
        println("Hello World")
        val inputFile = "file:///home/dictator/work_space/sparkSQL/data/input.txt"
        val outputDir = "file:///home/dictator/work_space/sparkSQL/out"
        val sparkConf = new SparkConf().setMaster("local").setAppName("SparkWordCountApp")
        val sc = new SparkContext(sparkConf)
        val rdd = sc.textFile(inputFile)
        // rdd.collect().foreach(println)
        rdd
            .flatMap(_.split(","))
            .map((_, 1))
            .reduceByKey(_+_)
            .map(x => (x._2, x._1))
            .sortByKey(false)
            .map(x => (x._2, x._1))
            // .collect().foreach(println)
            .saveAsTextFile(outputDir)
        sc.stop()
    }
}
