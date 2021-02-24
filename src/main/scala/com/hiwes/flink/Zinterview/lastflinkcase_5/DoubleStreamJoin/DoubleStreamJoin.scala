//package com.hiwes.flink.Zinterview.lastflinkcase_5.DoubleStreamJoin
//
//import java.util.Properties
//
//import org.apache.flink.api.common.functions.JoinFunction
//import org.apache.flink.api.common.serialization.DeserializationSchema
//import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
//import org.apache.flink.api.java.functions.KeySelector
//import org.apache.flink.streaming.api.TimeCharacteristic
//import org.apache.flink.streaming.api.functions.timestamps.{AscendingTimestampExtractor, BoundedOutOfOrdernessTimestampExtractor}
//import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
//import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
//import org.apache.flink.streaming.api.windowing.time.Time
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
//
///**
// * 1.双流Join问题.
// *
// * 双流Join属于高频问题，一般需要说明以下几点:
// * 1、Join大体分为2种: Window Join和Interval Join,其中Window Join又可根据Window的类型细分为3种:
// * Tumbling Window Join
// * Sliding Window Join
// * Session Window Join
// * 2、Windows类型的join是利用window的机制，先将数据存在WindowState中，窗口触发计算时，执行join操作;
// * 3、interval join也是利用State存储数据再处理，但区别在于State中数据有失效机制，依靠数据触发数据清理;
// * 4、目前Stream Join的结果是数据的笛卡尔积;
// * 5、日常使用中的一些问题，如: 数据延迟、Window序列化相关.
// *
// * @since hiwes by 2021/02/24
// */
//object DoubleStreamJoin {
//
//  /**
//   * 【双流Join和传统数据库Join的区别】
//   * 传统数据库表的Join是两张静态表的数据联接，在流上面是动态表，双流Join的数据不断流入.主要有3个核心区别:
//   * 1、左右两边的数据集合无穷.   传统数据库左右两个表的数据集合是有限的，双流Join数据会不断流入;
//   * 2、Join结果不断产生/更新.   传统数据库join是一次执行产生最终结果后退出，双流Join会持续不断的产生新的结果;
//   * 3、查询计算的双边驱动.      双流Join由于左右两边的流的速度不一样，会导致一边数据到来时可能另一边还没来,
//   * --- 所以在实现中，需要对左右两边的流数据进行保存，以保证Join的语义。在Blink中会以State的方式进行数据的存储.
//   *
//   * 【实例: 实现汇率和订单的双流Join】
//   * 场景描述:
//   * --- 购买外汇，外汇的汇率实时变化，用户在下单的同时，按最新的汇率计算成交价格.
//   * 需求说明:
//   * --- 构建2个数据流，一条是实时汇率，另一条是订单流，两条流合并，订单价格 * 汇率 计算出最终价格.
//   * 日志结构:
//   * --- 订单流: (时间戳:Long,商品大类:String,商品细目:Int,货币类型:String,价格:Int)
//   * --- 汇率流: (时间戳:Long,货币类型:String,汇率:Integer)
//   * 实现步骤:
//   * 1、创建订单流实时生成订单数据;
//   * 2、创建汇率流实时生成汇率数据;
//   * 3、双流Join，订单价格 * 汇率计算出最终价格:
//   * --- 3.1 初始化流处理环境
//   * --- 3.2 接入订单流，消费订单数据，并将消费的订单数据转换为元组对象
//   * --- 3.3 接入汇率流，消费汇率数据，并将消费的汇率数据转换为元组对象
//   * --- 3.4 设置按照事件时间处理数据
//   * --- 3.5 设置并行度为1
//   * --- 3.6 将订单流和汇率流添加到环境
//   * --- 3.7 为订单流和汇率流添加水印支持，延迟10ms
//   * --- 3.8 将订单流和汇率流进行合并，计算最终的订单价格进行输出，格式如下:
//   * （1585099091127,D,1,BEF,688,1585099094066,BEF,14,9632）
//   * （1585099094771,A,1,BEF,253,1585099094066,BEF,14,3542）
//   */
//  def main(args: Array[String]): Unit = {
//    val env = StreamExecutionEnvironment.getExecutionEnvironment
//
//    import org.apache.flink.api.scala._
//
//    val prop = new Properties()
//    prop.setProperty("bootstrap.servers", "hiwes:9092")
//    prop.setProperty("group.id", "test2-flink")
//    prop.setProperty("enable.auto.commit", "true")
//    prop.setProperty("auto.commit.interval.ms", "1000")
//    prop.setProperty("auto.offset.reset", "earliest")
//    prop.setProperty("session.timeout.ms", "30000")
//    prop.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
//    prop.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
//
//    // RateSource.
//    val consumerRate = new FlinkKafkaConsumer011("rate", new DeserializationSchema[Any] {
//      override def deserialize(message: Array[Byte]): (Long, String, Int) = {
//        val res = new String(message).split(",")
//        val timestamp = res(0).toLong
//        val dm = res(1)
//        val value = res(2).toInt
//        (timestamp, dm, value)
//      }
//
//      override def isEndOfStream(nextElement: Any): Boolean = false
//
//      override def getProducedType: TypeInformation[(Long, String, Int)] = {
//        TypeInformation.of(new TypeHint[(Long, String, Int)] {}) // 改动1
//      }
//    }, prop)
//
//    // OrderSource.
//    val consumerOrder = new FlinkKafkaConsumer011("order", new DeserializationSchema[Any] {
//      override def deserialize(message: Array[Byte]): (Long, String, Int, String, Int) = {
//        val res = new String(message).split(",")
//        val timestamp = res(0).toLong
//        val catlog = res(1)
//        val subcat = res(2).toInt
//        val dm = res(3)
//        val value = res(4).toInt
//        (timestamp, catlog, subcat, dm, value)
//
//      }
//
//      override def isEndOfStream(nextElement: Any): Boolean = false
//
//      override def getProducedType: TypeInformation[(Long, String, Int, String, Int)] = {
//        TypeInformation.of(new TypeHint[(Long, String, Int, String, Int)]() {})
//      }
//    }, prop)
//
//    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime) // 设置事件时间
//    env.setParallelism(1)
//
//    val rateStream: DataStream[(Long, String, Int)] = env.addSource(consumerRate)
//
//    val orderStream: DataStream[(Long, String, Int, String, Int)] = env.addSource(consumerOrder)
//
//    val delay = 1000
//
//    /**
//     * 为2个流添加事件时间
//     */
//    val rateTimedStream: DataStream[(Long, String, Int)] = rateStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[(Long, String, Int)](Time.milliseconds(delay)) {
//      override def extractTimestamp(element: (Long, String, Int)): Long = element._1.toLong
//    })
//
//    val orderTimedStream: DataStream[(Long, String, Int, String, Int)] = orderStream.assignTimestampsAndWatermarks(new AscendingTimestampExtractor[(Long, String, Int, String, Int)] {
//      override def extractAscendingTimestamp(element: (Long, String, Int, String, Int)): Long = element._1.toLong
//    })
//
//    /**
//     * 进行双流合并.在wher和equalTo中设置连接条件，通过window设置时间窗口，通过apply将join的结果拼接起来.
//     */
//    //    val joinedStream = orderTimedStream.join(rateTimedStream)
//    //      .where(new KeySelector[(Long, String, Int, String, Int), String] {
//    //        override def getKey(value: (Long, String, Int, String, Int)): String = value._4
//    //      })
//    //      .equalTo(new KeySelector[(Long, String, Int), String] {
//    //        override def getKey(value: (Long, String, Int)): String = value._2
//    //      }).window(TumblingEventTimeWindows.of(Time.seconds(10)))
//    //      .apply(new JoinFunction[(Long, String, Int, String, Int), (Long, String, Int), (Long, String, Int, String, Int, Long, String, Int, Int)] {
//    //        override def join(first: (Long, String, Int, String, Int), second: (Long, String, Int)): (Long, String, Int, String, Int, Long, String, Int, Int) = {
//    //          val res = second._3 * first._5
//    //          (first._1, first._2, first._3, first._4, first._5, second._1, second._2, second._3, res)
//    //        }
//    //      })
//
//    val joinedStream: DataStream[(Long, String, Int, String, Int, Long, String, Int, Int)] = orderTimedStream.join(rateTimedStream).where(x => new RecordSelect().getKey(x))
//      .equalTo(x => new RecordSelect2().getKey(x)).window(TumblingEventTimeWindows.of(Time.seconds(10)))
//      .apply(new JoinFunction[(Long, String, Int, String, Int), (Long, String, Int), (Long, String, Int, String, Int, Long, String, Int, Int)] {
//        override def join(first: (Long, String, Int, String, Int), second: (Long, String, Int)): (Long, String, Int, String, Int, Long, String, Int, Int) = {
//          val res = second._3 * first._5
//          (first._1, first._2, first._3, first._4, first._5, second._1, second._2, second._3, res)
//        }
//      })
//
//    joinedStream.print()
//
//    env.execute("done.")
//
//  }
//
//
//  class RecordSelect extends KeySelector[(Long, String, Int, String, Int), String] {
//    private final val serialVersionUID = 1L
//
//    override def getKey(value: (Long, String, Int, String, Int)): String = value._4
//  }
//
//  class RecordSelect2 extends KeySelector[(Long, String, Int), String] {
//    private final val serialVersionUID = 1L
//
//    override def getKey(value: (Long, String, Int)): String = value._2
//  }
//
//}
