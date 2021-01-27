package com.hiwes.flink.dataset

import java.io.File
import java.util

import org.apache.commons.io.FileUtils
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.FileSystem.WriteMode

/**
 * DataSet API 分布式缓存Distributed Cache 测试。
 */
object DataSetDistributedCacheTest {

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    // step1：注册cached file
    env.registerCachedFile("file:///Users/hiwes/data/test1.txt", "localFile")

    val data = env.fromElements("hadoop", "spark", "flink", "pyspark", "storm")

    val sink = data.map(new RichMapFunction[String, String] {
      // step2：在open方法中获取到分布式缓存的内容
      override def open(parameters: Configuration): Unit = {

        val myFile: File = getRuntimeContext.getDistributedCache().getFile("localFile")
        // val list = FileUtils.readLines(myFile).toArray()
        val list: util.List[String] = FileUtils.readLines(myFile)
        import scala.collection.JavaConverters._ // 将Java的List转为Scala.
        for (s <- list.asScala) {
          println(s)
        }
      }

      override def map(in: String): String = {
        in
      }
    })

    sink.writeAsText("file:///Users/hiwes/data/flink/dsTest.txt", WriteMode.OVERWRITE)

    env.execute("DataSetDistributedCacheTest")

  }

}
