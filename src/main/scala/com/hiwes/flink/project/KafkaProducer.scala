package com.hiwes.flink.project

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import scala.util.Random

object KafkaProducer {

  def main(args: Array[String]): Unit = {
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "hiwes:9092")
    properties.setProperty("key.serializer", classOf[StringSerializer].getName)
    properties.setProperty("value.serializer", classOf[StringSerializer].getName)

    val producer = new KafkaProducer[String, String](properties)

    val topic = "test"

    // 死循环，不停生产数据
    while (true) {
      val builder = new StringBuilder()

      builder.append("imooc").append("\t")
        .append("CN").append("\t")
        .append(getLevels()).append("\t")
        .append(new SimpleDateFormat("yyy-MM-dd HH:mm:ss").format(new Date())).append("\t")
        .append(getIPs()).append("\t")
        .append(getDomains()).append("\t")
        .append(getTraffic()).append("\t")

      println(builder.toString())

      producer.send(new ProducerRecord[String, String](topic, builder.toString()))
      Thread.sleep(2000)
    }
  }

  // 生产level数据
  def getLevels(): String = {
    val levels = Array[String]("M", "E", "E", "E", "E")
    levels(new Random().nextInt(levels.length))
  }

  // 生产ip数据
  def getIPs(): String = {
    val ips = Array[String](
      "223.104.18.110",
      "113.101.75.194",
      "27.17.127.135",
      "183.225.139.16",
      "112.1.66.34",
      "175.148.211.190",
      "183.227.58.21",
      "59.82.198.84",
      "117.28.38.28",
      "117.59.39.169"
    )
    ips(new Random().nextInt(ips.length))
  }

  // 生产域名数据
  def getDomains(): String = {
    val doamins = Array[String](
      "v1.go2yd.com",
      "v2.go2yd.com",
      "v3.go2vd.com",
      "v4.go2vd.com",
      "vmi.go2vd.com"
    )
    doamins(new Random().nextInt(doamins.length))
  }

  // 生成流量数据
  def getTraffic(): Long = {
    new Random().nextInt(10000).toLong
  }

}
