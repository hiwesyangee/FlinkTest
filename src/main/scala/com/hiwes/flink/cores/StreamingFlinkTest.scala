package com.hiwes.flink.cores

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * 使用Scala批处理实时数据。
 *
 * WordCount统计的数据，源自Socket。
 */
object StreamingFlinkTest {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val text: DataStream[String] = env.socketTextStream("hiwes", 9999)

    import org.apache.flink.api.scala._

    text.flatMap(_.toLowerCase.split(","))
      .filter(_.nonEmpty)
      .map((_, 1))
      .keyBy(0)
      .timeWindow(Time.milliseconds(3000))
      .sum(1)
      .print()
      .setParallelism(1)

    // 不设置并行度，前面就会有1> 3> 6> 7> 这种东西。

    env.execute("StreamingWordCount")

  }

  def testStreaming(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val text: DataStream[String] = env.socketTextStream("hiwes", 9999)

    import org.apache.flink.api.scala._

    text.flatMap(_.toLowerCase().split(","))
      .filter(_.nonEmpty)
      .map((_, 1))
      .keyBy(0)
      .timeWindow(Time.milliseconds(3000))
      .sum(1)
      .print()
      .setParallelism(1)

    env.execute("StreamingWordCount")
  }

}
