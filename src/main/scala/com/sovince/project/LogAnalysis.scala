package com.sovince.project

import java.text.SimpleDateFormat
import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.slf4j.LoggerFactory

/**
  * Created by vince
  * Email: so_vince@outlook.com
  * Data: 2019/6/25
  * Time: 20:15
  * Description:
  */
object LogAnalysis {
  private val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", Constants.BOOTSTRAP_SERVER)
    properties.setProperty("group.id", Constants.GROUP_ID)

    val source = new FlinkKafkaConsumer010[String](Constants.TOPIC,new SimpleStringSchema(),properties)

    env
      .addSource(source)
      .map(x=>{
        val fields = x.split("\t")
        val area = fields(1)
        val level = fields(2)
        var time = 0L
        try {
          time = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(fields(3)).getTime
        }catch {
          case e:Exception=>{
            logger.error(e.getMessage)
          }
        }
        var ip = fields(4)
        val domain = fields(5)
        val traffic = fields(6)
        (area,level,time,domain,traffic)
      })
      .print()

    env.execute("LogAnalysis")
  }

}
