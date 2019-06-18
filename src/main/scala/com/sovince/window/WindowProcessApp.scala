package com.sovince.window

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * Created by vince
  * Email: so_vince@outlook.com
  * Data: 2019/6/17
  * Time: 19:55
  * Description:
  */
object WindowProcessApp extends App {
  val env = StreamExecutionEnvironment.getExecutionEnvironment

  val text = env.socketTextStream("localhost", 9999)

  text
    .flatMap(_.split("\\s+"))
    .map((_, 0L))
    .keyBy(_._1)
    .timeWindow(Time.seconds(5))
    .process(new ProcessWindowFunction[(String, Long), String, String, TimeWindow] {
      override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[String]): Unit = {
        var count = 0L
        for (in <- elements) {
          count = count + 1
        }
        out.collect(s"Window ${context.window} count: $count")
        println("~~~~~~")
      }
    })
    .print()
    .setParallelism(1)

  env.execute("WindowProcessApp")


}
