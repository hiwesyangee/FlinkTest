package com.hiwes.flink.dataset

import org.apache.flink.api.common.accumulators.LongCounter
import org.apache.flink.api.common.functions.{RichFilterFunction, RichMapFunction}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.FileSystem.WriteMode

/**
 * DataSet API 计数器count 测试。
 */
object DataSetCountTest {
  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val data = env.fromElements("hadoop", "spark", "flink", "pyspark", "storm")

    // todo 这种map的方式，是肯定不能用来计数的，因为一旦并行度提高之后，直接造成的结果就是多个分区数据都从0开始。
    //    data.map(new RichMapFunction[String, Long] {
    //      var counter = 0l
    //
    //      override def map(in: String): Long = {
    //        counter = counter + 1
    //        counter
    //      }
    //    }).setParallelism(3).print()

    /**
     * 所有需用户定义的函数，都可以转变为RichMapFunction.
     * RichFuction除了提供原来MapFuction的方法之外，还提供:
     * open, close, getRuntimeContext 和setRuntimeContext方法.
     * 这些功能可用于参数化函数（传递参数），创建和完成本地状态，访问广播变量以及访问运行时信息以及有关迭代中的信息。
     */
    data.map(new RichMapFunction[String, String] {
      val count = new LongCounter()

      override def open(parameters: Configuration): Unit = {
        getRuntimeContext.addAccumulator("test_count", count)
      }

      override def map(value: String): String = {
        count.add(1)
        value
      }

    })

    val shuju = data.filter(_.contains("s")).collect()

    data.filter(new RichFilterFunction[String] {
      private var process: Seq[String] = null

      override def open(parameters: Configuration): Unit = {
        process = shuju
      }

      override def filter(value: String): Boolean = {
        process.contains(value)
      }
    })

    val info: DataSet[String] = data.map(new RichMapFunction[String, String] {

      // step1：定义计数器
      val count = new LongCounter()

      override def open(parameters: Configuration): Unit = {
        // step2：注册计数器
        getRuntimeContext.addAccumulator("ele-counts-scala", count)
      }

      override def map(in: String): String = {
        count.add(1)
        in
      }
    }).setParallelism(5)

    info.writeAsText("file:///Users/hiwes/data/flink/countTest.txt", WriteMode.OVERWRITE).setParallelism(5)

    val job = env.execute("DataSetCountTest")

    // step3：获取计数器结果
    val num: Long = job.getAccumulatorResult[Long]("ele-counts-scala")
    println(num)

    data.map(new RichMapFunction[String, String] {
      // 定义计数器
      val counter = new LongCounter()

      override def open(parameters: Configuration): Unit = {
        getRuntimeContext.addAccumulator("ownCounter", counter)
      }

      override def map(in: String): String = {
        counter.add(1)
        in
      }
    })


    //    data.print()
  }

}
