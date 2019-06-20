package com.sovince.stream.connectors


import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.fs.StringWriter
import org.apache.flink.streaming.connectors.fs.bucketing.{BucketingSink, DateTimeBucketer}
import org.apache.hadoop.conf.Configuration

/**
  * Created by vince
  * Email: so_vince@outlook.com
  * Data: 2019/6/19
  * Time: 19:38
  * Description:
  */
object FileSystemSinkApp {

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "root")
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val data = env.socketTextStream("localhost",9999)

    data.print().setParallelism(1)

//    val filePath = "output/hdfs/sink01/"
    val filePath = "hdfs://bigdata000/output/flink/sink01/"
    val sink = new BucketingSink[String](filePath)
//    val conf = new Configuration()
//    conf.set("hadoop.security.authentication", "Kerberos")
//    sink.setFSConfig(conf)
    sink.setBucketer(new DateTimeBucketer("yyyy-MM-dd--HHmm"))
    sink.setWriter(new StringWriter())
//    sink.setBatchSize(1024 * 1024 * 400) // this is 400 MB,
//    sink.setBatchRolloverInterval(20 * 60 * 1000); // this is 20 mins
    sink.setBatchRolloverInterval(20)
    sink.setBatchSize(1024)

    data.addSink(sink)

    env.execute(this.getClass.getSimpleName)
  }
}
