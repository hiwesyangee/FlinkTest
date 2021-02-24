package com.hiwes.flink.Zinterview.lastflinkcase_5.BloomFilterTest

import java.util.Properties

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.{CheckpointConfig, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.kafka.clients.consumer.ConsumerConfig

/**
 * Flink Utils工具类
 */
object FlinkUtils {
  private val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

  def getEnv(): StreamExecutionEnvironment = env


  /**
   * 从Kafka中读取数据，满足Exaclty-Once.
   */
  def createKafkaStream(parameter: ParameterTool, topic: String, groupId: String): DataStream[String] = {
    env.getConfig.setGlobalJobParameters(parameter)
    env.enableCheckpointing(parameter.getLong("checkpoint.interval", 5000L), CheckpointingMode.EXACTLY_ONCE)

    //    env.getConfig.setRestartStrategy(RestartStrategies.fixedDelayRestart(10,Time.seconds(20)))
    env.getConfig.setRestartStrategy(RestartStrategies.fixedDelayRestart(10, 20000l))
    env.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

    val prop = new Properties()
    prop.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, parameter.getRequired("bootstrap.server"))
    prop.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    prop.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, parameter.get("auto.offset.rest", "earliest"))
    prop.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, parameter.get("enable.auto.commit", "false"))

    val kafkaConsumer = new FlinkKafkaConsumer011[String](topic, new SimpleStringSchema(), prop)

    env.addSource(kafkaConsumer)
  }

}
