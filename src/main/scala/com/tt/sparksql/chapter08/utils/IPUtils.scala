package com.tt.sparksql.chapter08.utils

object IPUtils {

  def ip2Long(ip: String) = {
    println(">>>>>>>>>>>>>>>")
    println(ip)
    val splits: Array[String] = ip.split("[.]")
    var ipNum = 0l

    for ( i <- 0 until(splits.length) ) {
      ipNum = splits(i).toLong | ipNum << 8L
    }

    ipNum
  }

  def main(args: Array[String]): Unit = {
    println(ip2Long("182.91.190.221"))
  }
}
