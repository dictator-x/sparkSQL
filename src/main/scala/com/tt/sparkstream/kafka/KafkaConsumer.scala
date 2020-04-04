package com.tt.sparkstream.kafka

import org.apache.kafka.clients.consumer.{ ConsumerConfig, ConsumerRecords, ConsumerRecord }
import java.util.{ Properties }

class KafkaConsumer(val topic: String) extends Thread {

  val properties = new Properties()
  properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaProperties.BOOTSTRAP_SERVERS)
  properties.put("group.id", KafkaProperties.GROUP_ID)
  properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
  properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

  val consumer = new org.apache.kafka.clients.consumer.KafkaConsumer[String, String](properties)
  import scala.collection.JavaConverters._
  consumer.subscribe(List(topic).asJava)

  override def run() = {
    while (true) {
      var records: ConsumerRecords[String, String] = consumer.poll(100)
      records.iterator.asScala.foreach((record) => {
        println(s"offset = ${record.offset}, key = ${record.key}, value = ${record.value}")
      })
    }
  }
}
