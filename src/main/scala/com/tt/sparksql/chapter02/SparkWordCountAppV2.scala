package com.tt.sparksql.chapter02

import org.apache.spark.{SparkConf, SparkContext}

object SparkWordCountAppV2 {
    def main(args: Array[String]): Unit = {
        println("Hello World")
        val inputFile = args(0)
        val outputDir = args(1)
        val sparkConf = new SparkConf()
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
