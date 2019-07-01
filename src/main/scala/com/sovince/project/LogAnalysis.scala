package com.sovince.project

import java.text.SimpleDateFormat
import java.util.Properties

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.util.Collector
import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests
import org.slf4j.LoggerFactory

/**
  * Created by vince
  * Email: so_vince@outlook.com
  * Data: 2019/6/25
  * Time: 20:15
  * Description:
  */
object LogAnalysis {
  private val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 使用EventTime
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", Constants.BOOTSTRAP_SERVER)
    properties.setProperty("group.id", Constants.GROUP_ID)

    val source = new FlinkKafkaConsumer010[String](Constants.TOPIC, new SimpleStringSchema(), properties)

    //接收并过滤数据
    val logWithLevelE = env
      .addSource(source)
      .map(x => {
        val fields = x.split("\t")
        val area = fields(1)
        val level = fields(2)
        var time = 0L
        try {
          time = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(fields(3)).getTime
        } catch {
          case e: Exception => {
            logger.error(e.getMessage)
          }
        }
        var ip = fields(4)
        val domain = fields(5)
        val traffic = fields(6).toLong
        (area, level, time, domain, traffic)
      })
      .filter(_._2 == "E")
    //    logWithLevelE.print()


    //设置EventTime watermark
    val logAssignedWatermarks = logWithLevelE.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[(String, String, Long, String, Long)] {
      private val maxOutOfOrderness = 3500 // 3.5 seconds
      private var currentMaxTimestamp = 0L

      override def getCurrentWatermark: Watermark = {
        new Watermark(currentMaxTimestamp - maxOutOfOrderness)
      }

      override def extractTimestamp(element: (String, String, Long, String, Long), previousElementTimestamp: Long): Long = {
        val timestamp = element._3
        currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp)
        timestamp
      }
    })

    //EventTime窗口统计功能
    val logWindowed = logAssignedWatermarks
      .keyBy(3) //domain字段分组
      .window(TumblingEventTimeWindows.of(Time.seconds(60)))
      .apply(new WindowFunction[(String, String, Long, String, Long), (String, String, Long), Tuple, TimeWindow] {
        override def apply(key: Tuple, window: TimeWindow, input: Iterable[(String, String, Long, String, Long)], out: Collector[(String, String, Long)]): Unit = {
          val domain = key.getField(0).toString
          //          val domain = "kkk"
          var sum = 0L
          var maxTime = 0L

          val iterator = input.iterator
          while (iterator.hasNext) {
            val next = iterator.next()
            sum += next._5
            maxTime = Math.max(maxTime, next._3)
          }
          val batchTime = new SimpleDateFormat("yyyy-MM-dd HH:mm").format(maxTime)

          out.collect(batchTime, domain, sum)
        }
      })
    //        .print()

    //把数据Sink到ElasticSearch 构建esSink  (batchTime, domain, sum) = (String, String, Long)
    val httpHosts = new java.util.ArrayList[HttpHost]
    httpHosts.add(new HttpHost("bigdata000", 9200, "http"))
    val esSinkBuilder = new ElasticsearchSink.Builder[(String, String, Long)](
      httpHosts,
      new ElasticsearchSinkFunction[(String, String, Long)] {
        def createIndexRequest(element: (String, String, Long)): IndexRequest = {
          val json = new java.util.HashMap[String, Any]
          json.put("time", element._1)
          json.put("domain", element._2)
          json.put("traffics", element._3)

          val id = element._1 + "-" + element._2
          return Requests.indexRequest()
            .index("cdn")
            .`type`("traffic")
            .id(id)
            .source(json)
        }
        override def process(t: (String, String, Long), runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
          requestIndexer.add(createIndexRequest(t))
        }
      }
    )
    esSinkBuilder.setBulkFlushMaxActions(1)//设置不批量

    //最终添加构建好的esSink
    logWindowed.addSink(esSinkBuilder.build())
    //
    env.execute("LogAnalysis")
  }

}
