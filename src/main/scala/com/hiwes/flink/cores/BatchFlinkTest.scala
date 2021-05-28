package com.hiwes.flink.cores

import org.apache.flink.api.scala.ExecutionEnvironment

/**
 * 使用Scala批处理离线数据。
 */
object BatchFlinkTest {
  def main(args: Array[String]): Unit = {
    val path = "file:///Users/hiwes/data/test1.txt"

    // 1.获取运行环境
    val env = ExecutionEnvironment.getExecutionEnvironment

    // 2.读取数据
    val text = env.readTextFile(path)

    // 3.引入隐式转换
    import org.apache.flink.api.scala._

    // 4.操作数据
    text.flatMap(_.toLowerCase.split("\t"))
      .filter(_.nonEmpty)
      .map((_, 1))
      .groupBy(0)
      .sum(1)
      .print()
  }

  def testBatch(): Unit = {
    val path = "file:///Users/hiwes/data/test1.txt"
    val env = ExecutionEnvironment.getExecutionEnvironment

    val text = env.readTextFile(path)

    import org.apache.flink.api.scala._

    text.flatMap(_.toLowerCase().split("\t"))
      .filter(_.nonEmpty)
      .map((_, 1))
      .groupBy(0)
      .sum(1)
      .print()
  }
  
}
