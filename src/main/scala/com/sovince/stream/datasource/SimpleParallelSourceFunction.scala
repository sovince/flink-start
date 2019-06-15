package com.sovince.stream.datasource

import org.apache.flink.streaming.api.functions.source.{ParallelSourceFunction, SourceFunction}

/**
  * Created by vince
  * Email: so_vince@outlook.com
  * Data: 2019/6/14
  * Time: 20:40
  * Description:
  */
class SimpleParallelSourceFunction extends ParallelSourceFunction[Long]{

  private var isRunning = true
  private var count = 0L

  override def run(ctx: SourceFunction.SourceContext[Long]): Unit = {
    while (isRunning){
      ctx.collect(count)
      count += 1
      Thread.sleep(500)
    }
  }

  override def cancel(): Unit = {
    isRunning = false
  }
}
