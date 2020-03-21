package com.tt.sparksql.chapter05

import com.typesafe.config.{ Config, ConfigFactory }

object ParamsApp extends App {

  val config: Config = ConfigFactory.load()
  val url: String = config.getString("db.default.url")
  println(url)
}
