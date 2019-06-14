package com.sovince.batch.cache

import org.apache.commons.io.FileUtils
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration

/**
  * Created by vince
  * Email: so_vince@outlook.com
  * Data: 2019/6/13
  * Time: 20:18
  * Description:
  * Distributed Cache
  */
object DCache {
  private val env = ExecutionEnvironment.getExecutionEnvironment

  def main(args: Array[String]): Unit = {
    env.registerCachedFile("input/wordcount.txt","wordcount")

    val data = env.fromCollection(
      List(
        "Hadoop","flink", "spark",
        "Scala", "Java", "PHP"
      )
    )

    data.map(new RichMapFunction[String,String] {
      var one:String=_

      override def open(parameters: Configuration): Unit = {
        val file = getRuntimeContext.getDistributedCache.getFile("wordcount")

        val lines = FileUtils.readLines(file)

        import scala.collection.JavaConverters._
        for(e<-lines.asScala){
          if(one==null){
            one = e
          }
          println(e)
        }

      }

      override def map(value: String): String = {
        value+one
      }
    }).print()
  }

}
