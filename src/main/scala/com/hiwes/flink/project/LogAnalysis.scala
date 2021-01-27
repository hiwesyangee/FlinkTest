package com.hiwes.flink.project

import org.apache.flink.api.scala._
import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.util.Collector
import org.apache.http.HttpHost
import org.apache.kafka.common.serialization.StringSerializer
import org.elasticsearch.action.index.IndexRequest
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer

/**
 * 需求1: 日志分析，统计一分钟内，每个域名访问产生的流量
 */
object LogAnalysis {

  // 生产上记录日志。
  val logger = LoggerFactory.getLogger("LogAnalysis")

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime) // 设置event-time.

    // 接收kafka数据
    val topic = "test"

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "hiwes:9092")
    properties.setProperty("key.serializer", classOf[StringSerializer].getName)
    properties.setProperty("value.serializer", classOf[StringSerializer].getName)
    properties.setProperty("group.id", "test-log-group")

    val consumer = new FlinkKafkaConsumer010[String](topic,
      new SimpleStringSchema(),
      properties)

    // 接收Kafka数据
    val data = env.addSource(consumer)

    val logData = data.map(x => {
      val arr = x.split("\t")
      val level = arr(2)
      val timeStr = arr(3) // 时间格式不友好，需要转换为long类型
      var time = 0l

      try {
        val sourceFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        time = sourceFormat.parse(timeStr).getTime
      } catch {
        case e: Exception => logger.error(s"time aprse errir: $timeStr", e.getMessage)
      }

      val domain = arr(5)
      val traffic = arr(6).toLong

      (level, time, domain, traffic)
    }).filter(_._2 != 0)
      .filter(_._1 == "E")
      .map(x => (x._2, x._3, x._4)) // 去掉level，因为剩下的，都是level=E的，抛弃E即可。
    logData.print().setParallelism(1)

    // 生产上进行业务处理时，需要考虑处理的健壮性及数据的准确性。
    // 脏数据和不符合业务规则的数据需要全部过滤后进行业务逻辑的处理。
    // 此处的 只需要统计level=E的即可，对于M的，不作为业务指标的统计范畴。
    // 数据清洗，就是按照业务规则把原始数据进行一定业务规则的处理，使得满足业务需求为准。

    // 增加时间和水印
    val resultData = logData.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[(Long, String, Long)] {
      val maxOutOfOrderness = 10000L // 10s
      var currentMaxTimestamp: Long = _

      override def getCurrentWatermark: Watermark = {
        new Watermark(currentMaxTimestamp - maxOutOfOrderness)
      }

      override def extractTimestamp(element: (Long, String, Long), previousElementTimestamp: Long): Long = {
        val timestamp = element._1
        currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp)
        timestamp
      }
    }).keyBy(1) //此处是按照域名进行keyBy的
      .window(TumblingEventTimeWindows.of(Time.seconds(60)))
      .apply(new WindowFunction[(Long, String, Long), (String, String, Long), Tuple, TimeWindow] {
        override def apply(key: Tuple, window: TimeWindow, input: Iterable[(Long, String, Long)], out: Collector[(String, String, Long)]): Unit = {

          val domain = key.getField(0).toString
          var sum = 0l

          val times = ArrayBuffer[Long]()

          val iterator = input.iterator
          while (iterator.hasNext) {
            val next = iterator.next()
            sum += next._3 // traffic求和

            // TODO... 是能拿到这个window里面的时间的  next._1
            times.append(next._1)
          }

          /**
           * 第一个参数：这一分钟的时间 2019-09-09 20:20
           * 第二个参数：域名
           * 第三个参数：traffic的和
           */
          val time = new SimpleDateFormat("yyyy-MM-dd HH:mm").format(new Date(times.max))
          out.collect((time, domain, sum))
        }
      })

    val httpHosts = new java.util.ArrayList[HttpHost]()
    httpHosts.add(new HttpHost("hiwes", 9300, "http"))

    val esSinkBuilder = new ElasticsearchSink.Builder[(String, String, Long)](
      httpHosts,
      new ElasticsearchSinkFunction[(String, String, Long)] {
        def createIndexRequest(element: (String, String, Long)): IndexRequest = {
          val json = new java.util.HashMap[String, Any]
          json.put("time", element._1)
          json.put("domain", element._2)
          json.put("traffics", element._3)

          val id = element._1 + "-" + element._2

          import org.elasticsearch.client.Requests
          Requests.indexRequest()
            .index("cdn")
            .`type`("traffic")
            .id(id)
            .source(json)
        }

        override def process(t: (String, String, Long), runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
          requestIndexer.add(createIndexRequest(t))
        }
      }
    )

    // 批量请求的配置;这指示接收器在每个元素之后发出，否则它们将被缓冲
    esSinkBuilder.setBulkFlushMaxActions(1)

    // 最后.构建并将接收器添加到作业管道中
    resultData.addSink(esSinkBuilder.build) //.setParallelism(5)

    //    resultData.print().setParallelism(1)

    env.execute("LogAnalysis")
  }

}
