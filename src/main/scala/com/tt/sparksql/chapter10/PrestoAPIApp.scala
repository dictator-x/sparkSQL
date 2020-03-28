package com.tt.sparksql.chapter10

import java.sql.{ DriverManager, Connection, Statement, ResultSet }

object PrestoAPIApp extends App {

  Class.forName("com.facebook.presto.jdbc.PrestoDriver")

  val connection = DriverManager.getConnection("jdbc:presto://centos:8080/hive/default", "dictator", "")
  val statement = connection.createStatement();
  val resultSet = statement.executeQuery("show tables");
  while ( resultSet.next() ) {
    println(resultSet.getString(1))
  }

}

