package com.hiwes.flink.timeandwindow

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object WindowFlinkTest {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val text = env.socketTextStream("hiwes", 9999)

    text.flatMap(_.split(","))
      .map((_, 1))
      .keyBy(0)
      //            .timeWindow(Time.seconds(3))
      .timeWindow(Time.seconds(10), Time.seconds(5))
      //      .countWindow()
      .sum(1)
      .print()
      .setParallelism(1)

    env.execute("WindowFlinkTest")
  }

}
