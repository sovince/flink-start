package com.sovince.batch.others

import org.apache.flink.api.common.accumulators.LongCounter
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.FileSystem.WriteMode

/**
  * Created by vince
  * Email: so_vince@outlook.com
  * Data: 2019/6/13
  * Time: 19:21
  * Description:
  */
object CounterApp {
  private val env = ExecutionEnvironment.getExecutionEnvironment

  def main(args: Array[String]): Unit = {

    var i = 1
    val data = env.fromCollection(
      List(
        "Hadoop","flink", "spark",
        "Scala", "Java", "PHP"
      )
    )

    val res = data.map(new RichMapFunction[String,String] {
      //1.定义一个计数器
      val counter = new LongCounter()

      //Default life cycle methods
      override def open(parameters: Configuration): Unit = {
        //2.注册计数器
        getRuntimeContext.addAccumulator("success-counter",counter)

      }

      override def map(value: String): String = {
        counter.add(1)
        value
      }

    })
//    res.print()
    res.writeAsText("output/sink02/",WriteMode.OVERWRITE).setParallelism(2)

    //从作业结果取出计数器
    val jobResult = env.execute("counter_test")
    val num = jobResult.getAccumulatorResult[Long]("success-counter")

    println("success-counter:"+num)


  }

}
