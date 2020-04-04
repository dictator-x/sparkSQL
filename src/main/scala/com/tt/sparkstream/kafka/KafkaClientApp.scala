package com.tt.sparkstream.kafka

object KafkaClientApp extends App {
  new KafkaProducer(KafkaProperties.TOPIC).start()
  new KafkaConsumer(KafkaProperties.TOPIC).start()
  // while(true){}
}

