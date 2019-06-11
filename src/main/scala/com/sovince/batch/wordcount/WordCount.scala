package com.sovince.batch.wordcount

import org.apache.flink.api.scala._

/**
  * Created by vince
  * Email: so_vince@outlook.com
  * Data: 2019/6/8
  * Time: 20:45
  * Description:
  */
object WordCount {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val text = env.readTextFile("input/wordcount.txt")
    
    text
      .flatMap(_.toLowerCase.split(" "))
      .map((_,1))
      .groupBy(0)
      .sum(1)
      .print()


  }

}
