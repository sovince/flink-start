package com.sovince.stream.datasource

import java.lang

import org.apache.flink.streaming.api.collector.selector.OutputSelector
import org.apache.flink.streaming.api.scala._

/**
  * Created by vince
  * Email: so_vince@outlook.com
  * Data: 2019/6/14
  * Time: 19:43
  * Description:
  */
object DataStreamSource {
  private val env = StreamExecutionEnvironment.getExecutionEnvironment


  def fromSocket(): Unit = {
    env.socketTextStream("localhost", 9999).print()
    env.execute("fromSocket")
  }

  /**
    * 非并行的自定义源
    */
  def simpleSourceFunction(): Unit = {
    env.addSource(new SimpleSourceFunction).print()
    env.execute("simpleSourceFunction")
  }

  def simpleParallelSourceFunction(): Unit = {
    env.addSource(new SimpleParallelSourceFunction).setParallelism(3).print()
    env.execute("simpleParallelSourceFunction")
  }

  def filterFunction(): Unit = {
    env
      .addSource(new SimpleSourceFunction)
      .map(x => {
        println("received:" + x)
        x
      })
      .filter(_ % 3 == 0)
      .print()
      .setParallelism(1)

    env.execute("filterFunction")
  }

  def unionAndSplitFunction(): Unit = {
    val data1 = env.addSource(new SimpleSourceFunction)
    val data2 = env.addSource(new SimpleSourceFunction)

    val all = data1.union(data2)
    //    all.split(x=>List(""))
    val split = all.split(x => x % 3 match {
        //返回到某个list
      case 0 => List("first")
      case 1 => List("second")
      case 2 => List("third")
    })
    val select = split.select("first","third")
    select.print()
    env.execute("unionAndSplitFunction")
  }


  def main(args: Array[String]): Unit = {
    //RichParallelSourceFunction 实现了AbstractRichFunction
    //RichSinkFunction也是实现了AbstractRichFunction
    //所以AbstractRichFunction。。。
    //多了open()和close()
    //SourceFunction接口提供了run()和cancel()方法的重写
    //SinkFunction接口提供了invoke()的重写

    unionAndSplitFunction()
  }

}
