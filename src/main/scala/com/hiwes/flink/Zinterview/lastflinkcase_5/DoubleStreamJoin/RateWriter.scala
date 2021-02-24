//package com.hiwes.flink.Zinterview.lastflinkcase_5.DoubleStreamJoin
//
//import java.util.{Properties, Random}
//
//import org.apache.flink.api.common.serialization.SimpleStringSchema
//import org.apache.flink.streaming.api.functions.source.SourceFunction
//import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011
//
///**
// * 汇率流: (时间戳:Long,货币类型:String,汇率:Integer)
// *
// * BEF：比利时法郎
// * CNY：人民币
// * DEM：德国马克
// * EUR：欧元
// * HKD：港币
// * USD：美元
// * ITL：意大利里拉
// */
//object RateWriter {
//
//  final val HBDM = Array("BEF", "CNY", "DEM", "EUR", "HKD", "USD", "ITL")
//
//  def main(args: Array[String]): Unit = {
//
//    import org.apache.flink.api.scala._
//    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    val prop = new Properties()
//    prop.setProperty("bootstrap.servers", "hiwes:9092")
//
//    val messageStream = env.addSource(new SourceFunction[String] {
//
//      private final val random = new Random()
//      private final val serialVersionUID: Long = 1L
//
//      private var isRun = true
//
//      override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
//        while (isRun) {
//          Thread.sleep(random.nextInt(3) * 1000)
//          ctx.collect(String.format("%d,%s,%d", System.currentTimeMillis(),
//            HBDM(random.nextInt(HBDM.length)), random.nextInt(20)))
//        }
//      }
//
//      override def cancel(): Unit = {
//        this.isRun = false
//      }
//    })
//
//   messageStream.addSink(new FlinkKafkaProducer011[String]("rate",
//        new SimpleStringSchema(),
//        prop)
//    )
//
//    messageStream.print()
//
//    env.execute("write rate to kafka.")
//  }
//}
