package com.hiwes.flink.Zinterview.core_1

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}

/**
 * 批处理单词统计.
 * 编写flink程序，读取文件中的字符串，并以\t进行单词拆分打印。
 *
 * @by hiwes since 2021/02/19
 */
object BatchWordCount {
  def main(args: Array[String]): Unit = {

    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val data: DataSet[String] = env.readTextFile("file:///Users/hiwes/data/test1.txt")

    import org.apache.flink.api.scala._

    data.flatMap(_.toLowerCase.split("\t"))
      .filter(_.nonEmpty)
      .map((_, 1))
      .groupBy(0)
      .sum(1)
      .print()

  }
}
