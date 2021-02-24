//package com.hiwes.flink.Zinterview.lastflinkcase_5.BloomFilterTest
//
//import java.time.{LocalDateTime, ZoneId}
//import java.time.format.DateTimeFormatter
//
//import com.google.common.hash.{BloomFilter, Funnels}
//import org.apache.flink.api.common.functions.{RichFlatMapFunction, RichMapFunction}
//import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
//import org.apache.flink.api.java.utils.ParameterTool
//import org.apache.flink.configuration.Configuration
//import org.apache.flink.streaming.api.TimeCharacteristic
//import org.apache.flink.streaming.api.datastream.{DataStream, SingleOutputStreamOperator}
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
//import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
//import org.apache.flink.streaming.api.windowing.time.Time
//import org.apache.flink.util.Collector
//
//
///**
// * 3.布隆过滤器,实现大量数据过滤问题.
// *
// * 【某用户，在一天中早上玩了游戏A、上午玩了游戏A和B、下午玩了游戏A、晚上玩了游戏B.
// * 那么该用户在这一天玩了几个游戏?】
// * 数据量偏少的时候，可以很快的分出来。
// *
// * 【数据结构】
// * (用户ID，日期，时间，游戏连接，参加次数）
// * (1011504891,2020-02-24,05:23:38,http://192.168.xxx.xxx:8088/v5.3/gameA.html,1)
// *
// * 【实时系统去重方案】
// * 1、使用Redis，将每条数据和Redis进行判断.
// * --- 缺陷: 每次都要通过网络连接Redis服务，网络速度明显慢于缓存速度，且网络也不稳定.
// * 2、使用HashSet，因为HashSet本身也无序且不重复.
// * --- 缺陷: 千万、亿万的数据写入HashSet，底层是通过Hash算法实现的，数据越多，效率越低.
// * 3、使用Flink结合布隆过滤器实现.
// *
// * 布隆过滤器，类似于一个HashSet，用于快速判断某个元素是否存在于集合中，典型案例是:
// * 快速判断一个key是否存在与某个容器中，不存在则直接返回.
// * 其关键在于:
// * Hash算法 和 容器大小（后者不用考虑，可以存足够的数据）.
// *
// * @since hiwes by 2021/02/24
// */
//object BloomFilterTest {
//  def main(args: Array[String]): Unit = {
//    import org.apache.flink.api.scala._
//    val env = StreamExecutionEnvironment.getExecutionEnvironment
//
//    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
//
//    val parameters = ParameterTool.fromPropertiesFile("file:///Users/hiwes/data/config.properties")
//    val kafkaStream: DataStream[String] = FlinkUtils.createKafkaStream(parameters, "test_log", "groupA")
//
//    val streamOperator = kafkaStream.assignTimestampsAndWatermarks(
//      new BoundedOutOfOrdernessTimestampExtractor[String](Time.seconds(0)) {
//        override def extractTimestamp(element: String): Long = {
//          val arr = element.split("\\|")
//          val time = arr(1)
//          val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
//          val parse = LocalDateTime.parse(time, formatter)
//          LocalDateTime.from(parse).atZone(ZoneId.systemDefault()).toInstant.toEpochMilli
//        }
//      }
//    )
//
//    val tuple4Operator: DataStream[(String, String, String, String)] = streamOperator.flatMap(new RichFlatMapFunction[String, (String, String, String, String)] {
//      override def flatMap(value: String, out: Collector[(String, String, String, String)]): Unit = {
//        val logSplit = value.split("\\|")
//
//        val dateTime = logSplit(1)
//        val dateTimeSplit = dateTime.split(" ")
//
//        val date = dateTimeSplit(0)
//        val time = dateTimeSplit(1)
//
//        val userId = logSplit(2)
//
//        val url = logSplit(3)
//
//        out.collect((userId, date, time, url))
//      }
//    })
//    //      .returns(Types.TUPLE(Types.STRING, Types.STRING, Types.STRING, Types.STRING))
//
//    val keyedStream = tuple4Operator.keyBy(0, 1)
//
//    /**
//     * 分组后，开始使用布隆过滤器进行去重.
//     */
//    val distinctStream: SingleOutputStreamOperator[(String, String, String, String, Long)] = keyedStream.map(new RichMapFunction[(String, String, String, String), (String, String, String, String, Long)] {
//      // 1.记录游戏State.
//      private var productState: ValueState[BloomFilter] = null
//      // 2.记录次数State.
//      private var countState: ValueState[Long] = null
//
//      override def open(parameters: Configuration): Unit = {
//        // 定义一个状态描述器BloomFilter
//        //        val stateDescriptor: ValueStateDescriptor[BloomFilter] = new ValueStateDescriptor[BloomFilter](
//        //          "product-state", Class[BloomFilter]
//        //        )
//
//
//        val stateDescriptor = new ValueStateDescriptor[BloomFilter](
//          "produce-state", Class[BloomFilter]
//        )
//
//        // 使用RuntimeContext获取状态.
//        productState = getRuntimeContext.getState(stateDescriptor)
//
//        // 定义一个状态描述器[次数]
//        val countDescriptor = new ValueStateDescriptor[Long](
//          "count-state", Class[Long]
//        )
//
//        // 使用RuntimeContext获取状态.
//        countState = getRuntimeContext.getState(countDescriptor)
//
//      }
//
//      override def map(value: (String, String, String, String)): (String, String, String, String, Long) = {
//        // 获取点击链接
//        val url: String = value._4
//        var bloomFilter = productState.value()
//
//        if (bloomFilter == null) {
//          // 初始化一个BloomFilter
//          bloomFilter = BloomFilter.create(Funnels.unencodedCharsFunnel(), 100000)
//          countState.update(0l)
//        }
//
//        // BloomFilter可以判断一定不包含
//        if (!bloomFilter.mightContain(url)) {
//          bloomFilter.put(url)
//          countState.update(countState.value() + 1)
//        }
//
//        // 更新productState
//        productState.update(bloomFilter)
//        (value._1, value._2, value._3, value._4, countState.value())
//
//      }
//    })
//    distinctStream.print("distinctStream >>> ")
//
//    env.execute("UserJoinGameAnalysis")
//
//  }
//}
