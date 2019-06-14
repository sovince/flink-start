package com.sovince.batch.sink

import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode

/**
  * Created by vince
  * Email: so_vince@outlook.com
  * Data: 2019/6/13
  * Time: 19:01
  * Description:
  */
object SinkApp {
  private val env = ExecutionEnvironment.getExecutionEnvironment

  def writeText(): Unit = {
    val data = env.fromCollection(1 to 9)

    data
      .writeAsText("output/sink01/", WriteMode.OVERWRITE)
      .setParallelism(2)

    //如果设置了并行度，sink01输出的是一个文件夹

    env.execute("sink")

  }

  def main(args: Array[String]): Unit = {
    writeText()
  }

}
