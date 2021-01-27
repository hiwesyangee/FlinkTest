package com.hiwes.flink.datastream

import org.apache.flink.streaming.api.scala._

object DataStreamParallelSourceTest {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //    nonParallelSourceFunction(env) // 这不 是一个并行的，在底层会判断并行度是否为1，不是1的话会直接报错。
    //    parallelSourceFunction(env) // 这是一个并行的
    richParallelSourceFunction(env) // 这是一个并行的

    env.execute("DataStreamTest")
  }


  def nonParallelSourceFunction(env: StreamExecutionEnvironment): Unit = {
    val data = env.addSource(new DataStreamCustomNonParallelSourceFunction)
    data.print().setParallelism(1)
  }

  def parallelSourceFunction(env: StreamExecutionEnvironment): Unit = {
    val data = env.addSource(new DataStreamCustomParallelSourceFunction).setParallelism(2)
    data.print()
  }

  def richParallelSourceFunction(env: StreamExecutionEnvironment): Unit = {
    val data = env.addSource(new DataStreamCustomRichParallelSourceFunction).setParallelism(2)
    data.print()
  }

  def socketFunction(env: StreamExecutionEnvironment): Unit = {
    env.socketTextStream("hiwes", 9999).print().setParallelism(1)
  }
}
