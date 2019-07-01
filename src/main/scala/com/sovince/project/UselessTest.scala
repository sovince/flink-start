package com.sovince.project

import org.apache.flink.streaming.api.scala._

/**
  * Created by vince
  * Email: so_vince@outlook.com
  * Data: 2019/7/1
  * Time: 21:50
  * Description:
  */
object UselessTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.addSource(new DomainUserMysqlSource).setParallelism(1).print().setParallelism(1)

    env.execute("UselessTest")
  }

}
