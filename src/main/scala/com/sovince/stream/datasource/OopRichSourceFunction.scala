package com.sovince.stream.datasource

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}

/**
  * Created by vince
  * Email: so_vince@outlook.com
  * Data: 2019/6/15
  * Time: 09:59
  * Description:
  */
class OopRichSourceFunction extends RichParallelSourceFunction{

  override def run(ctx: SourceFunction.SourceContext[Nothing]): Unit = {

  }

  override def cancel(): Unit = {

  }

  override def open(parameters: Configuration): Unit = {
//    getRuntimeContext
  }

  override def close(): Unit = {

  }
}
