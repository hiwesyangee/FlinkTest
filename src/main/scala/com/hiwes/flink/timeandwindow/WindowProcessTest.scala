package com.hiwes.flink.timeandwindow

import java.lang

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object WindowProcessTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val data = env.socketTextStream("hiwes", 9999)

    //    data.flatMap(_.split(","))
    //      .map(x => (1, x.toInt))
    //      .keyBy(0) // 此时key都是1，所有元素都到一个task去执行。
    //      .timeWindow(Time.seconds(3))
    //      //      .timeWindow(Time.seconds(10), Time.seconds(5))
    //      //      .countWindow()
    //      .process(new ProcessWindowFunction[Tuple2[Integer, Integer], Any, Tuple1, TimeWindow] {
    //        override def process(key: Tuple1, context: Context, elements: lang.Iterable[(Integer, Integer)], out: Collector[Any]): Unit = {
    //          println("~~~~~~~~~")
    //          var count = 0
    //          for (in <- elements) {
    //            count = count + 1
    //          }
    //          out.collect("Window: " + context.window() + "count: " + count)
    //        }
    //
    //      })
    //
    //      //      .process(new ProcessWindowFunction[Tuple2[Integer,Integer],Any,Tuple,TimeWindow] {
    //      //        override def process(key: Tuple, context: Context, elements: Iterable[Tuple2[Integer, Integer]], out: Collector[Any]): Unit = {
    //      //          println("~~~~~~~~~");
    //      //          var count = 0;
    //      //          for (in <- elements) {
    //      //            count = count+1
    //      //          }
    //      //          out.collect("Window: " + context.window() + "count: " + count);
    //      //
    //      //        }
    //      //      })
    //
    //      .print()
    //      .setParallelism(1)

    env.execute("WindowProcessTest")
  }


}
