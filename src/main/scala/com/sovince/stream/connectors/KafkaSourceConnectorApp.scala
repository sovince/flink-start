package com.sovince.stream.connectors

import java.util.{Date, Properties}

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer010, FlinkKafkaProducer010}

/**
  * Created by vince
  * Email: so_vince@outlook.com
  * Data: 2019/6/20
  * Time: 21:49
  * Description:
  */
object KafkaSourceConnectorApp {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    sourceConnector(env)


    env.execute("KafkaSourceConnectorApp")
  }

  def sourceConnector(env: StreamExecutionEnvironment): Unit = {
    //kafka-console-producer.sh --broker-list bigdata000:9092 --topic test
    val topic = "test"
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "bigdata000:9092")
    // only required for Kafka 0.8
    //  properties.setProperty("zookeeper.connect", "bigdata000:2181")
    properties.setProperty("group.id", "test")

    val sourceFromKafka = new FlinkKafkaConsumer010[String](topic, new SimpleStringSchema(), properties)
//    sourceFromKafka.setStartFromEarliest()
//    sourceFromKafka.setStartFromLatest()
//    sourceFromKafka.setStartFromTimestamp(new Date().getTime)
    sourceFromKafka.setStartFromGroupOffsets()
    val data = env
      .addSource(
        sourceFromKafka
      )
      .print()
  }


}
