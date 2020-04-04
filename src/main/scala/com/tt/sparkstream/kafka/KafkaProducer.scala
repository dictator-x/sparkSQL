package com.tt.sparkstream.kafka

import org.apache.kafka.clients.producer.{ Producer, ProducerConfig, ProducerRecord };
import java.util.Properties;

class KafkaProducer(val topic: String) extends Thread {

  val properties = new Properties()
  properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaProperties.BOOTSTRAP_SERVERS)
  properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
  properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

  val producer = new org.apache.kafka.clients.producer.KafkaProducer[String, String](properties)

  override def run() = {
    var messageNo = 1
    while(true) {
      val key = "key_" + messageNo
      var message = "message_" + messageNo
      producer.send(new ProducerRecord[String, String](topic, key, message))
      println("Send: " + message)

      messageNo += 1

      Thread.sleep(2000)
    }
  }
}
