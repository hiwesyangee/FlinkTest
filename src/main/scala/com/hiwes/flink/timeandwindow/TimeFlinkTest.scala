package com.hiwes.flink.timeandwindow

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.TimeCharacteristic

/**
 * Flink Time 测试.
 */
object TimeFlinkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

  }

}
