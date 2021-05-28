package com.hiwes.flink.Zinterview.core_1

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * 流处理单词统计.
 * 编写flink程序，接收socket的单词数据，并以\t进行单词拆分打印。
 *
 * @by hiwes since 2021/02/19
 */
object StreamingWordCount {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val data: DataStream[String] = env.socketTextStream("localhost", 9999)

    import org.apache.flink.api.scala._

    data.flatMap(_.toLowerCase().split("\t"))
      .filter(_.nonEmpty)
      .map((_, 1))
      .keyBy(0)
      .timeWindow(Time.milliseconds(2000))
      .sum(1)
      .print()
      .setParallelism(1)

    env.execute()

  }
}
