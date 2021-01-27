package com.hiwes.flink.datastream

import java.{lang, util}

import org.apache.flink.streaming.api.collector.selector.OutputSelector
import org.apache.flink.streaming.api.scala._

/**
 * DataStream API Transform测试.
 */
object DataStreamTransformationTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //    mapAndFilterFunction(env)
    //    unionFunction(env)
    splitAndSelect(env)

    env.execute("DataStreamTransformationTest")
  }

  // map filter
  def mapAndFilterFunction(env: StreamExecutionEnvironment): Unit = {
    val data = env.addSource(new DataStreamCustomRichParallelSourceFunction)
    data.map(x => x).filter(_ % 2 == 0).print().setParallelism(1)
  }

  // union
  def unionFunction(env: StreamExecutionEnvironment): Unit = {
    val data1 = env.addSource(new DataStreamCustomRichParallelSourceFunction)
    val data2 = env.addSource(new DataStreamCustomRichParallelSourceFunction)
    data1.union(data2).print().setParallelism(1)
  }

  // split select
  def splitAndSelect(env: StreamExecutionEnvironment): Unit = {
    val data = env.addSource(new DataStreamCustomRichParallelSourceFunction)
    val splits: SplitStream[Long] = data.split(new OutputSelector[Long] {
      override def select(value: Long): lang.Iterable[String] = {
        val list = new util.ArrayList[String]()
        if (value % 2 == 0) list.add("even")
        else list.add("odd")
        list
      }
    })

    val even: DataStream[Long] = splits.select("even")
    even.print().setParallelism(1)

  }
}
