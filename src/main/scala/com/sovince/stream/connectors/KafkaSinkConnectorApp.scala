package com.sovince.stream.connectors

import java.util.Properties

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
object KafkaSinkConnectorApp {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    sinkConnector(env)


    env.execute("KafkaSourceConnectorApp")
  }


  def sinkConnector(env: StreamExecutionEnvironment): Unit = {
    val data = env.socketTextStream("localhost", 9999)

    val topic = "test"
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "bigdata000:9092")
//    val sinkToKafka = new FlinkKafkaProducer010[String]("bigdata000:9092", topic, new SimpleStringSchema)
    val sinkToKafka = new FlinkKafkaProducer010[String](topic,new SimpleStringSchema(),properties)
    sinkToKafka.setWriteTimestampToKafka(true)

//    val sinkToKafka = new FlinkKafkaConsumer010[String](
//      topic,
//      new KeyedSerializationSchemaWrapper[String](new SimpleStringSchema()),
//      properties
//    )

    data
      .map(x=>{
        println("received from Socket:"+x)
        x
      })
      .addSink(sinkToKafka)

    //kafka-console-consumer.sh --bootstrap-server bigdata000:9092 --topic test
  }

}
