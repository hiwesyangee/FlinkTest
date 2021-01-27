package com.hiwes.flink.dataset

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration

/**
 * DataSet API 广播变量broadcast 测试。
 */
object DataSetBroadcastTest {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val toBroadcast = env.fromElements(1, 2, 3)

    val data = env.fromElements("a", "b")
    val need: DataSet[String] = data.map(new RichMapFunction[String, String] {
      var broadcastSet: Traversable[String] = null

      override def open(parameters: Configuration): Unit = {
        import scala.collection.JavaConverters._
        broadcastSet = getRuntimeContext.getBroadcastVariable[String]("broadcastSetName").asScala
      }

      override def map(in: String): String = in
    }).withBroadcastSet(toBroadcast, "broadcastSetName")

    for (elem <- need.collect()) {
      println(elem)
    }

//    env.execute("DataSetBroadcastTest")
  }
}
