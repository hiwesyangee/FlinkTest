package com.hiwes.flink.Zinterview.highlevel_4

import org.apache.flink.api.common.functions.{FilterFunction, MapFunction}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * Flink水印实例.
 * 统计: 统计每30内单词出行的次数(滑动窗口、消息的最大延迟时间是5秒)
 *
 * since 2021-04-01 by hiwes.
 */
object FlinkHighLevel_WaterMark {
  def main(args: Array[String]): Unit = {

    val senv = StreamExecutionEnvironment.getExecutionEnvironment

    senv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    senv.setParallelism(1)

    val sourceDS = senv.socketTextStream("hiwes", 9999)

    // 过滤非法数据，如（23432-aa）等.
    val filterDS = sourceDS.filter(new FilterFunction[String] {
      override def filter(value: String): Boolean = {
        if (null == value || "".equals(value))
          return false
        val lines = value.split(",")
        if (lines.length != 2) {
          return false
        }
        true
      }
    })

    import org.apache.flink.streaming.api.scala._
    // 做map转换，从string到tuple3，时间、单词、次数.
    val mapDS = filterDS.map(new MapFunction[String, Tuple3[Long, String, Int]] {
      override def map(value: String): (Long, String, Int) = {
        val lines = value.split(",")
        new Tuple3[Long, String, Int](lines(0).toLong, lines(1), 1)
      }
    })

    // 设置watermark生成方式为periodic watermark，实现他的两个函数getCurrentWatermark和extractTimestamp
    val wordcountDS = mapDS.assignTimestampsAndWatermarks(
      new AssignerWithPeriodicWatermarks[(Long, String, Int)] {
        var currentMaxTimestamp = 0l

        val maxOutOfOrderness = 5000l

        override def getCurrentWatermark: Watermark = {
          new Watermark(currentMaxTimestamp - maxOutOfOrderness)
        }

        override def extractTimestamp(element: (Long, String, Int), previousElementTimestamp: Long): Long = {
          val ts = element._1
          currentMaxTimestamp = Math.max(ts, currentMaxTimestamp)
          ts
        }
      }).keyBy(0)
      .timeWindow(Time.seconds(30))
      .sum(2)

    wordcountDS.print("\n 单词统计:")

    senv.execute("Window WordCount.")

  }
}
