package com.hiwes.flink.dataset

import org.apache.flink.api.scala._
import org.apache.flink.api.scala.operators.ScalaCsvOutputFormat
import org.apache.flink.core.fs.FileSystem.WriteMode

/**
 * DataSet API的Sink测试。
 */
object DataSetSinkTest {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val data: Seq[Int] = 1 to 10
    val text = env.fromCollection(data)

    val filePath = "file:///Users/hiwes/data/flink/sinkTest.txt"

    text.writeAsText(filePath, WriteMode.OVERWRITE)
    // .setParallelism(2) // 当设置多个并行度的时候，会直接写入到文件夹，sinkTest.txt就成了文件夹名

    text.writeAsCsv(filePath,
      ScalaCsvOutputFormat.DEFAULT_LINE_DELIMITER,
      ScalaCsvOutputFormat.DEFAULT_FIELD_DELIMITER,
      WriteMode.OVERWRITE)

    env.execute("sinkTest")
  }

}
