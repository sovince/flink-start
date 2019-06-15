package com.sovince.stream.datasource

import org.apache.flink.streaming.api.functions.source.SourceFunction

/**
  * Created by vince
  * Email: so_vince@outlook.com
  * Data: 2019/6/14
  * Time: 20:25
  * Description:
  */
class SimpleSourceFunction extends SourceFunction[Long]{

  private var count:Long = 0L

  private var isRunning:Boolean = true

  override def run(ctx: SourceFunction.SourceContext[Long]): Unit = {
    while (isRunning){
      ctx.collect(count)
      count += 1
      Thread.sleep(500)
    }
  }

  override def cancel(): Unit = {
    isRunning=false
  }
}
