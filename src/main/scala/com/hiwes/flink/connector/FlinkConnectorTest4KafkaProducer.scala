package com.hiwes.flink.connector

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010

/**
 * Flink Connector测试: Kafka作为Sink.
 */
object FlinkConnectorTest4KafkaProducer {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // checkpoint常用设置参数。
    // 为流作业启用检查点。对流数据流的分布式状态进行周期性快照。
    // 在失败的情况下，流数据流将从最近完成的检查点重新启动。
    env.enableCheckpointing(5000) // 每5s进行一次checkpoint
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setCheckpointTimeout(10000) // 超时时间
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1) // 最大并发

    val data = env.socketTextStream("hiwes", 9999)

    val producer = new FlinkKafkaProducer010[String]("hiwes:9092", "test", new SimpleStringSchema())

    // 启用此功能将使生产者仅记录Exception，而不是捕获并重新抛出Exception。
    producer.setLogFailuresOnly(true)

    // 启用此功能后，Flink的检查点将在Kafka确认检查点时等待所有即时记录，然后再执行检查点。
    // 这样可以确保将检查点之前的所有记录都写入Kafka。至少一次必须启用。
    producer.setFlushOnCheckpoint(true)

    // FlinkKafkaProducer010 只发出了记录时间戳
    producer.setWriteTimestampToKafka(true)

    data.addSink(producer)

    env.execute("FlinkConnectorTest4KafkaProducer")

  }

}
