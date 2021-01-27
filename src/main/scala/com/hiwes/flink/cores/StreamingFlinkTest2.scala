package com.hiwes.flink.cores

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * 使用Scala处理实时数据2。
 *
 * WordCount统计的数据，源自Socket。
 * 增加参数的外部获取方式。
 */
object StreamingFlinkTest2 {
  def main(args: Array[String]): Unit = {

    var host = ""
    var port = 0

    try {
      val tool = ParameterTool.fromArgs(args)
      host = tool.get("host")
      port = tool.getInt("port")
    } catch {
      case e: Exception => {
        System.err.println("端口未设置,使用默认host: localhost,和默认端口: 9999.")
        host = "localhost"
        port = 9999
      }
    }

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //    StreamExecutionEnvironment.createLocalEnvironment()   // 创建本地的执行环境，包含默认参数：并行度
    //    StreamExecutionEnvironment.createRemoteEnvironment("hiwes", 8181) // 创建远程执行环境，包含默认参数：hote、port、并行度、压缩包，一般用的比较少。


    val text: DataStream[String] = env.socketTextStream(host, port)

    import org.apache.flink.api.scala._

    text.flatMap(_.toLowerCase.split(","))
      .filter(_.nonEmpty)
      .map((_, 1))
      .keyBy(0)
      .timeWindow(Time.seconds(3))
      .sum(1)
      .print()
      .setParallelism(1)

    // 不设置并行度，前面就会有1> 3> 6> 7>这种东西。

    env.execute("StreamingWordCount")

  }
}
