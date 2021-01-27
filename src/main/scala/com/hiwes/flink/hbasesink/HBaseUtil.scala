package com.hiwes.flink.hbasesink

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.client.ConnectionFactory

object HBaseUtil {
  def getConnection(zkQuorum: String, port: Int): Connection = {
    val conf = new Configuration()
    conf.set("hbase.zookeeper.quorum", "hiwes")
    conf.set("hbase.zookeeper.property.clientPort", "2181")

    ConnectionFactory.createConnection(conf)
  }
}