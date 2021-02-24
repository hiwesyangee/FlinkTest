package com.hiwes.flink.Zinterview.highlevel_4

import java.sql.{Connection, DriverManager, SQLException, Timestamp}
import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeutils.base.VoidSerializer
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.flink.streaming.api.functions.sink.{SinkFunction, TwoPhaseCommitSinkFunction}

/**
 * 8.2 MySQL来实现端到端的Exactly Once语义.
 *
 * 需求:
 * 1、Checkpoint每10s进行1次，此时用FlinkKafkaConsumer
 * 2、消费并处理完消息后，进行一次预提交数据库的操作.
 * --- 2.1 如果预提交没有问题，10s后进行真正的插入数据库操作，如果插入成功，进行一次checkpoint，
 * ------ flink会自动记录消费的offset，可以将checkpoint保存的数据放到hdfs中
 * --- 2.2 如果预提交出错，比如在5s的时候出错了，此时Flink程序就会进入不断的重启中，重启的策略可以在配置中设置，
 * ------ checkpoint记录的还是上一次成功消费的offset，本次消费的数据因为在checkpoint期间，消费成功，但是预提交过程中失败了
 * --- 2.3 注意此时数据并没有真正的执行插入操作，因为预提交（preCommit）失败，提交（commit）过程也不会发生。
 * ------等将异常数据处理完成之后，再重新启动这个Flink程序，它会自动从上一次成功的checkpoint中继续消费数据，以此来达到Kafka到Mysql的Exactly-Once。
 *
 */
@SuppressWarnings("all")
object StreamDemoKafka2Mysql {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 设置并行度，为了方便测试，查看消息的顺序，这里设置为1，可以更改为多并行度
    env.setParallelism(1)

    // 每隔10s进行启动一个检查点【设置checkpoint的周期】
    env.enableCheckpointing(10000)

    // 设置模式为：exactly_one，仅一次语义
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)

    // 确保检查点之间有1s的时间间隔【checkpoint最小间隔】
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(1000)

    // 检查点必须在10s内完成，或者被丢弃【Checkpoint超时时间】
    env.getCheckpointConfig.setCheckpointTimeout(10000)

    // 同一时间只允许1个checkpoint
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)

    // 表示一旦Flink程序被cancel后，会保留checkpoint数据，以便根据实际需要恢复到指定的checkpoint
    env.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

    // 设置statebackend，将检查点保存在hdfs上，默认保存在内存.先保存在本地.
    env.setStateBackend(new FsStateBackend("file:///Users/hiwes/data/output/tmp/cp/"))

    val prop = new Properties()
    prop.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hiwes:9092")
    prop.put(ConsumerConfig.GROUP_ID_CONFIG, "flink-consumer-group1")

    // SimpleStringSchema可以获取到kafka消息.
    // JSONKeyValueDeserializationSchema可以获取都消息的key,value，metadata:topic,partition，offset等信息
    //    val kafkaConsumer011 = new FlinkKafkaConsumer011[String]("test2", new SimpleStringSchema(), prop)
    val kafkaConsumer011 = new FlinkKafkaConsumer011[ObjectNode]("test2",
      new JSONKeyValueDeserializationSchema(true), prop)

    import org.apache.flink.api.scala._
    val streamSource = env.addSource(kafkaConsumer011)


    streamSource.addSink(new MySqlTwoPhaseCommitSink()).name("MySqlTwoPhaseCommitSink")

    env.execute(StreamDemoKafka2Mysql.getClass.getName)

  }
}


class MySqlTwoPhaseCommitSink() extends TwoPhaseCommitSinkFunction[ObjectNode, Connection, Void](new KryoSerializer[Connection](classOf[Connection], new ExecutionConfig), VoidSerializer.INSTANCE) {
  /**
   * 执行数据入库操作
   *
   * @paramconnection
   * @paramobjectNode
   * @paramcontext
   * @throwsException
   */
  @throws[Exception]
  override protected def invoke(connection: Connection, objectNode: ObjectNode, context: SinkFunction.Context[_]): Unit = {

    System.err.println("startinvoke.......")
    val date = new SimpleDateFormat("yyyy-MM-ddHH:mm:ss").format(new Date)
    System.err.println("===>date:" + date + "" + objectNode)
    val value = objectNode.get("value").toString
    val sql = "insertinto`t_test`(`value`,`insert_time`)values(?,?)"
    val ps = connection.prepareStatement(sql)
    ps.setString(1, value)
    ps.setTimestamp(2, new Timestamp(System.currentTimeMillis))
    //执行insert语句
    ps.execute
    //手动制造异常
    if (value.toInt == 15) System.out.println(1 / 0)
  }

  /**
   * 获取连接，开启手动提交事物（getConnection方法中）
   *
   * @return
   * @throwsException
   */
  @throws[Exception]
  override protected def beginTransaction: Connection = {
    val url = "jdbc:mysql://hiwes:3306/test?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull&useSSL=false&autoReconnect=true"
    val connection = new DBConnectUtils().getConnection(url, "root", "root")
    System.err.println("start beginTransaction......." + connection)
    connection
  }

  /**
   * 预提交，这里预提交的逻辑在invoke方法中
   *
   * @paramconnection
   * @throwsException
   */
  @throws[Exception]
  override protected def preCommit(connection: Connection): Unit = {
    System.err.println("start preCommit......." + connection)
  }

  /**
   * 如果invoke执行正常则提交事物
   *
   * @paramconnection
   */
  override protected def commit(connection: Connection): Unit = {
    System.err.println("start commit......." + connection)

    new
    new DBConnectUtils().commit(connection)
  }

  override protected def recoverAndCommit(connection: Connection): Unit = {
    System.err.println("start recoverAndCommit......." + connection)
  }

  override protected def recoverAndAbort(connection: Connection): Unit = {
    System.err.println("start abort recoverAndAbort......." + connection)
  }

  /**
   * 如果invoke执行异常则回滚事物，下一次的checkpoint操作也不会执行
   *
   * @paramconnection
   */
  override protected def abort(connection: Connection): Unit = {
    System.err.println("startabortrollback......." + connection)
  }
}

class DBConnectUtils {
  // 获取连接
  def getConnection(url: String, user: String, password: String): Connection = {
    var conn: Connection = null

    try {
      Class.forName("com.mysql.jdbc.Driver")
    } catch {
      case e: ClassNotFoundException => e.printStackTrace()
    }

    conn = DriverManager.getConnection(url, user, password)

    // 设置手动提交
    conn.setAutoCommit(false)
    conn
  }

  // 提交事务
  def commit(conn: Connection): Unit = {
    if (conn != null) {
      try {
        conn.commit()
      } catch {
        case e: SQLException => e.printStackTrace()
      } finally {
        close(conn)
      }
    }
  }

  // 事务回滚
  def rollback(conn: Connection): Unit = {
    if (conn != null) {
      try {
        conn.rollback()
      } catch {
        case e: SQLException => e.printStackTrace()
      } finally {
        close(conn)
      }
    }
  }

  // 关闭连接
  def close(conn: Connection): Unit = {
    if (conn != null) {
      try {
        conn.close()
      } catch {
        case e: SQLException => e.printStackTrace()
      }
    }
  }

}

