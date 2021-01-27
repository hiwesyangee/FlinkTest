package com.hiwes.flink.connector

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition

/**
 * Flink Connector测试: kafka作为Source.
 */
object FlinkConnectorTest4KafkaConsumer {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // checkpoint常用设置参数。
    // 为流作业启用检查点。对流数据流的分布式状态进行周期性快照。
    // 在失败的情况下，流数据流将从最近完成的检查点重新启动。
    env.enableCheckpointing(5000) // 每5s进行一次checkpoint
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setCheckpointTimeout(10000) // 超时时间
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1) // 最大并发

    // Flink只有在有足够的处理槽可用来重启拓扑时才能重启拓扑。
    // 因此，如果由于丢失TaskManager而导致拓扑失败，那么之后一定还有足够的可用插槽。
    // Flink on YARN支持自动重新启动丢失的纱线容器。
    // 如果检查点没有启用，Kafka使用者将定期向Zookeeper提交偏移量。

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "hiwes:9092")
    properties.setProperty("zookeeper.connect", "hiwes:2181")
    properties.setProperty("group.id", "test")

    val consumer = new FlinkKafkaConsumer010[String](
      "test",
      // 使用这种定于所有与正则表达式匹配的主题（以一个数字开头，以一个数字结尾）
      // 如果需要在开始运行后发现动态创建的topic，就需要设置一个非负值:
      // flink.partition-discovery.interval-millis
      // 从而可以发现名称和指定正则表达式相匹配的新topic的分区。
      //      java.util.regex.Pattern.compile("test-topic-[0-9]"),
      new SimpleStringSchema(),
      properties)

    consumer.setCommitOffsetsOnCheckpoints(false) // 开启checkpoing之后配置是否需要提交偏移量，默认true

    // 配置kafka consumer的开始位置
    //    consumer.setStartFromEarliest()   // 从最早的记录开始。
    //    consumer.setStartFromLatest()   // 从最新的记录开始。
    //    consumer.setStartFromTimestamp(startupOffsetsTimestamp: Long)  // 从指定的时间戳开始。
    //    consumer.setStartFromGroupOffsets()   // 【默认】开始从消费者组（group.id在消费者属性中的设置）中在Kafka broker（或Kafka 0.8的Zookeeper）中提交的偏移中读取分区。如果找不到分区的偏移量，auto.offset.reset则将使用属性中的设置。

    // 可以为每个分区指定consumer应该从哪开始的确切偏移量。
    val specificStartOffsets = new java.util.HashMap[KafkaTopicPartition, java.lang.Long]()
    specificStartOffsets.put(new KafkaTopicPartition("test", 0), 23L)
    specificStartOffsets.put(new KafkaTopicPartition("test", 1), 31L)
    specificStartOffsets.put(new KafkaTopicPartition("test", 2), 43L)
    consumer.setStartFromSpecificOffsets(specificStartOffsets) // 将使用者配置为从topic的分区0、1和2的指定偏移量开始读取。偏移值应该是使用者应为每个分区读取的下一条记录。
    // 当作业从故障中自动还原或使用保存点手动还原时，这些起始位置配置方法不会影响起始位置。
    // 还原时，每个Kafka分区的起始位置由保存点或检查点中存储的偏移量确定

    val data = env.addSource(consumer)
    data.print().setParallelism(1)

    env.execute("FlinkConnectorTest4KafkaConsumer")

  }

}
