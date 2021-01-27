package com.hiwes.flink.timeandwindow

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time


object WindowReduceTest {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val text = env.socketTextStream("hiwes", 9999)

    // 原来传递进来的数据是String，现在测试用数值类型，来测试增量的效果
    text.flatMap(_.split(","))
      .map(x => (1, x.toInt))
      .keyBy(0) // 此时key都是1，所有元素都到一个task去执行。
      .timeWindow(Time.seconds(3))
      //      .timeWindow(Time.seconds(10), Time.seconds(5))
      //      .countWindow()
      .reduce((x1, x2) => (x1._1, x1._2 + x2._2))
      .print()
      .setParallelism(1)

    env.execute("WindowReduceTest")
  }

}
