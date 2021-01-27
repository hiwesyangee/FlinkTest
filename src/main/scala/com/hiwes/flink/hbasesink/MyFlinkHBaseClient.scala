package com.hiwes.flink.hbasesink

import java.util.Properties

import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object MyFlinkHBaseClient {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val source = new MyHbaseSource

    import org.apache.flink.api.scala._
    val data = env.addSource(source)
    data.print().setParallelism(1)

    val result: DataStream[String] = data.map(x => {
      val id: String = x._1
      val n1 = x._2
      id + "test" + "\t" + n1
    })

    result.print().setParallelism(1)

    result.addSink(new MyHbaseSink)

    env.execute("MyFlinkHBaseClient")

  }

  // 从HBase读取数据
  def readFromHBaseWithRichSourceFunction(): Unit = {
    import org.apache.flink.api.scala._
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(5000)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    val dataStream = env.addSource(new MyHbaseSource)
    dataStream.map(x => println(x._1 + " " + x._2))
    env.execute()
  }

}
