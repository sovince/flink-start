package com.sovince.window

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * Created by vince
  * Email: so_vince@outlook.com
  * Data: 2019/6/17
  * Time: 19:01
  * Description:
  */
object WindowReduceApp extends App {
  val env = StreamExecutionEnvironment.getExecutionEnvironment

  val text = env.socketTextStream("localhost",9999)

  text
    .flatMap(_.split("\\s+")).map(_.toInt)
    .map(x=>{
      if(x%2==0){
        (0,x)
      } else{
        (1,x)
      }
    })
    .keyBy(0)
    .timeWindow(Time.seconds(5))
    .reduce((t1,t2)=> {//属于增量处理，效率高
      (t1._1,t1._2+t2._2)
    })
    .print()

  env.execute("WindowReduceApp")

}
