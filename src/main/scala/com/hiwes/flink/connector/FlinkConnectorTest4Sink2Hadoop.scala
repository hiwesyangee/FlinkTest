package com.hiwes.flink.connector

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.fs.{StringWriter}
import org.apache.flink.streaming.connectors.fs.bucketing.{BucketingSink, DateTimeBucketer}

/**
 * Flink Connector测试: Sink到Hadoop FileSystem.
 * 注意，这种方式写出来的文件，都会很小，从而出现小文件问题，是会有大问题的。
 */
object FlinkConnectorTest4Sink2Hadoop {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val data: DataStream[String] = env.socketTextStream("hiwes", 9999)

    val localFilePath = "file:///Users/hiwes/data/flink/socket"
    val hdfsFilePath = "hdfs://hiwes:9002/data/flink"
    //    val sink = new BucketingSink[String](localFilePath)
    //    data.setParallelism(1).addSink(sink)

    val sink: BucketingSink[String] = new BucketingSink[String](localFilePath)
    sink.setBucketer(new DateTimeBucketer[String]("yyyy-MM-dd--HHmm"))
    sink.setWriter(new StringWriter())
    //    sink.setBatchSize(1024 * 1024 * 400) // this is 400 MB,
    //    sink.setBatchRolloverInterval(20 * 60 * 1000); // this is 20 mins
    sink.setBatchRolloverInterval(20) // 20s
    data.addSink(sink)

    env.execute("FlinkConnectorTest4Sink2Hadoop")

  }

}
