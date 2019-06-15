package com.sovince.stream.datasink

import org.apache.flink.streaming.api.scala._

/**
  * Created by vince
  * Email: so_vince@outlook.com
  * Data: 2019/6/15
  * Time: 14:02
  * Description:
  */
object Sink2Mysql {
  private val env = StreamExecutionEnvironment.getExecutionEnvironment

  def sink2Mysql(): Unit = {
    val data = env.socketTextStream("localhost", 9999)
    data
      .map(x => {
        println("received:" + x)
        val fields = x.split(",")
        City(0L, fields(0).toInt, fields(1))
      })
      //      .print()
      .addSink(new Sink2MysqlCity())


    env.execute("sink2Mysql")
  }

  def main(args: Array[String]): Unit = {
    sink2Mysql()
  }

}
