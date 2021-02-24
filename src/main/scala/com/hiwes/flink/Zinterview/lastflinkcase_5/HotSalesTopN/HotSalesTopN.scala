//package com.hiwes.flink.Zinterview.lastflinkcase_5.HotSalesTopN
//
//import java.util.Properties
//
//import org.apache.flink.api.common.serialization.DeserializationSchema
//import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
//import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
//
///**
// * 2.热点销售排行TopN问题.
// * TopN分为全局topN、分组topN。
// *
// * @since hiwes by 2021/02/24
// */
//object HotSalesTopN {
//
//  /**
//   * 【实例: 实时热门商品————每隔十秒输出最近30s点击量最多的前N个商品】
//   */
//  def main(args: Array[String]): Unit = {
//    val env = StreamExecutionEnvironment.getExecutionEnvironment
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
//    val consumerRate = new FlinkKafkaConsumer011[(Long,String,Int)]("rate", new DeserializationSchema[Any] {
//      override def deserialize(message: Array[Byte]): Any = {
//        val res = new String(message).split(",")
//        val timestamp = res(0).toLong
//        val dm = res(1)
//        val value = res(2).toInt
//        (timestamp, dm, value)
//      }
//
//      override def isEndOfStream(nextElement: Any): Boolean = {
//        false
//      }
//
//      override def getProducedType: TypeInformation[Any] = {
//        TypeInformation.of(new TypeHint[(Long, String, Int)] {})
//      }
//    })
//
//
//  }
//
//}
