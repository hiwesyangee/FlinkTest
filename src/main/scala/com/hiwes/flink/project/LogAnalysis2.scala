package com.hiwes.flink.project

import java.text.SimpleDateFormat
import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

import scala.collection.mutable

/**
 * 需求2: 日志分析，统计一分钟内，每个用户产生的流量；
 * 还需要读取Mysql中的域名映射
 */
object LogAnalysis2 {
  val logger = LoggerFactory.getLogger("LogAnalysis2")

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    import org.apache.flink.api.scala._
    val topic = "test"

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "hiwes:9092")
    properties.setProperty("group.id", "test-flink-1")

    val consumer = new FlinkKafkaConsumer010[String](topic, new SimpleStringSchema(), properties)

    val data = env.addSource(consumer)

    val logData: DataStream[(Long, String, Long)] = data.map(x => {
      val arr = x.split("\t")
      val level = arr(2)

      val timeStr = arr(3)
      var time = 0l

      try {
        val sourceFormat = new SimpleDateFormat("yyyy-MM-dd HH-mm-ss")
        time = sourceFormat.parse(timeStr).getTime
      } catch {
        case e: Exception => {
          logger.error(s"time parse error: $timeStr", e.getMessage)
        }
      }

      val domain = arr(5)
      val traffic = arr(6).toLong

      (level, time, domain, traffic)
    }).filter(_._2 != 0).filter(_._1 == "E")
      .map(x => (x._2, x._3, x._4))

    val mysqlData: DataStream[mutable.HashMap[String, String]] = env.addSource(new MySQLSource)

    /**
     * CoFlatMapFunction在两个连接的流上实现平面映射转换。
     * 转换函数的同一个实例被用来转换两个连接的流。这样，流转换可以共享状态。
     * 使用连接流的一个例子是将随时间变化的规则应用到流的元素上。
     * 其中一个连接的流有规则，另一个流有应用规则的元素。对连接的流的操作保持状态中的当前规则集。
     * 它可以接收规则更新(来自第一个流)并更新状态，或者:
     * 接收数据元素(来自第二个流)并将状态中的规则应用于元素。应用规则的结果将被发出。
     */
    val connectData: DataStream[String] = logData.connect(mysqlData)
      .flatMap(new CoFlatMapFunction[(Long, String, Long), mutable.HashMap[String, String], String] {
        var userDomainMap = mutable.HashMap[String, String]()

        // log
        override def flatMap1(value: (Long, String, Long), out: Collector[String]): Unit = {
          val domain = value._2
          val userId = userDomainMap.getOrElse(domain, "")
          print("~~~~" + userId)
          out.collect(value._1 + "\t" + value._2 + "\t" + value._3 + "\t" + userId)
        }

        // MySQL
        override def flatMap2(value: mutable.HashMap[String, String], out: Collector[String]): Unit = {
          userDomainMap = value
        }
      })

    connectData.print()

    env.execute("LogAnalysis2")

  }
}
