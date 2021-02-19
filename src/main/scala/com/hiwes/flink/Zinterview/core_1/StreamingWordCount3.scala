package com.hiwes.flink.Zinterview.core_1

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * 流处理单词统计3.
 * 编写flink程序，接收socket的单词数据，并以\t进行单词拆分打印。
 * 增加参数的外部获取方式.
 * 使用字段表达式定义K.
 *
 * @by hiwes since 2021/02/19
 */
object StreamingWordCount3 {
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
    val stream = env.socketTextStream(host, port)

    import org.apache.flink.api.scala._

    stream.flatMap(_.toLowerCase().split("\t"))
      .filter(_.nonEmpty)
      .map(x => WC(x, 1))
      .keyBy("word")
      .timeWindow(Time.milliseconds(2000))
      .sum(1)
      .print()
      .setParallelism(1)

    env.execute("StreamingFlinkTest3")

  }

  case class WC(word: String, count: Int)

}
