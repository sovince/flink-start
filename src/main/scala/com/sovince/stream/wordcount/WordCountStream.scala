package com.sovince.stream.wordcount

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * Created by vince
  * Email: so_vince@outlook.com
  * Data: 2019/6/8
  * Time: 20:54
  * Description:
  */
object WordCountStream {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val params = ParameterTool.fromArgs(args)

    var host = ""
    var port = 0

    if(params.has("h")&&params.has("p")){
      host = params.get("h")
      port = params.getInt("p")
    }else{
      print("Missing params of host and port,set to default")
      print("-h localhost -p 9999")
      host = "localhost"
      port = 9999
    }

    val text = env.socketTextStream(host,port)
    text
      .flatMap(_.toLowerCase.split(" "))
      .map((_,1))
      .keyBy(0)
      .timeWindow(Time.seconds(5))
      .sum(1)
      .print()
      .setParallelism(2)

    env.execute("wordcount stream job")
  }

}
