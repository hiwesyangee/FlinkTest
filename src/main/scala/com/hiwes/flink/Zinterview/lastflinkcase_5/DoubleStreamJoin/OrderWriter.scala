//package com.hiwes.flink.Zinterview.lastflinkcase_5.DoubleStreamJoin
//
//import java.util.{Properties, Random}
//
//import org.apache.flink.api.common.serialization.SimpleStringSchema
//import org.apache.flink.streaming.api.functions.source.SourceFunction
//import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011
//
///**
// * 订单流: (时间戳:Long,商品大类:String,商品细目:Int,货币类型:String,价格:Int)
// */
//object OrderWriter {
//  def main(args: Array[String]): Unit = {
//
//    import org.apache.flink.api.scala._
//    val env = StreamExecutionEnvironment.getExecutionEnvironment
//
//    val prop = new Properties()
//    prop.setProperty("bootstrap.servers", "hiwes:9092")
//
//    val messageStream: DataStream[String] = env.addSource(new SourceFunction[String] {
//      private final val random = new Random()
//
//      private final val serialVersionUID: Long = 1L
//
//      private var isRUn = true
//
//      override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
//        while (isRUn) {
//          Thread.sleep(random.nextInt(1500))
//          val catlog: Char = (65 + random.nextInt(5)).toChar
//          ctx.collect(String.format("%d,%s,%d,%s,%d",
//            System.currentTimeMillis(), String.valueOf(catlog), random.nextInt(5),
//            RateWriter.HBDM(random.nextInt(RateWriter.HBDM.length)),
//            random.nextInt(1000)
//          ))
//        }
//      }
//
//      override def cancel(): Unit = {
//        this.isRUn = false
//      }
//    })
//
//    messageStream.addSink(new FlinkKafkaProducer011[String]("order",
//      new SimpleStringSchema(),
//      prop
//    ))
//
//    messageStream.print()
//
//    env.execute("write order to kafka.")
//  }
//}
