package com.tt.sparksql.chapter06

import java.sql.{ DriverManager, Connection, PreparedStatement, ResultSet }

object JDBCClientApp {

  def main(args: Array[String]) = {

    Class.forName("org.apache.hive.jdbc.HiveDriver")

    // just keep password empty in local
    val conn: Connection = DriverManager.getConnection("jdbc:hive2://centos:10000", "dictator", "")
    val pstmt: PreparedStatement = conn.prepareStatement("select * from helloworld");
    val rs: ResultSet = pstmt.executeQuery()

    while( rs.next() ) {
      println(rs.getObject(1) + " : " + rs.getObject(2))
    }
  }

}
