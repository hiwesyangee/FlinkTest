package com.hiwes.flink.Zinterview.datastream_3

import java.{lang, util}
import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.regex.Pattern
import java.util.{Properties, UUID}

import org.apache.flink.api.common.serialization.{SimpleStringEncoder, SimpleStringSchema, TypeInformationSerializationSchema}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.functions.SemanticPropUtil
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.{FileSystem, Path}
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.collector.selector.OutputSelector
import org.apache.flink.streaming.api.functions.co.CoMapFunction
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.BasePathBucketAssigner
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy
import org.apache.flink.streaming.api.functions.source.{ParallelSourceFunction, RichParallelSourceFunction, RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer010, FlinkKafkaProducer010, KafkaSerializationSchema}
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema
import org.apache.flink.util.NumberSequenceIterator
import org.apache.kafka.clients.producer.ProducerRecord

import scala.util.Random

/**
 * DataStream API.
 *
 * @by hiwes since 2021/02/20
 */
object DataStream1 {
  def main(args: Array[String]): Unit = {
    // 1.流处理的概念.
    // 2.DataStream API及基本流程.的Transformation.

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 3.DataStream的source.
    //    source4DataStream(env)

    // 4.DataStream的Transformation.
    //    transformations4DataStream(env)

    // 5.DataStream的Sink.
    sinks4DataStream(env)

  }

  // 1.流处理的概念.
  /**
   * 采用数据驱动的方式，提前设置一些算子，然后等数据到达后对数据进行处理，分布式流引擎（Spark、Flink）都是采用DAG图来表示整个计算逻辑.
   * 其中DAG图中的每个点代表一个基本的逻辑单元，也就是算子。由于计算逻辑被组织成有向图，数据按照边的方向，从一些特殊的Source节点流入系统，
   * 然后通过网络传输、本地传输等不同的方式在算子间进行发送和处理，最后通过Sink节点将结果发送到某个外部系统或数据库中.
   *
   * 注意: 只有当算子实例分布到不同进程上时，才需要通过网络进行数据传输，
   * 而同一进程中的多个实例之间的数据传输通常不需要通过网络.
   *
   * Flink的接口虽然也在构建计算逻辑图，但是Flink的API定义更加面向数据本身的处理逻辑，将数据流抽象为一个无限集，
   * 然后定义了一组集合上的操作，然后在底层自动构建相应的DAG图.
   * = = = = 》 Flink的API要更加上层一些.
   */

  // 2.DataStream API及基本流程.的Transformation.
  /**
   * 基于Flink的DataStream API来编写流处理程序一般需3步:
   * 1、通过Source接入数据;
   * 2、进行一系列的处理及将数据写出;
   * 3、进行显式调用execute(),否则前面的逻辑不会真正执行.
   *
   * 一般来说包含以下几类:
   * 1、对单条记录的操作: filter、map等;
   * 2、对多条记录的操作: Window等;
   * 3、对多个流转换为单个流: union、join、connect等;
   * 4、DataStream还支持合并对称的操作，即: 把一个流按一定规则拆分为多个流Split、每个流都是之前流的一个子集.
   */

  // 3.DataStream的source.
  /**
   * flink提供了一个方法: 通过env对象调用addSource()，可以定义source.
   * 参数为一个SourceFunction接口的实例化对象.
   * Flink自带了许多实现的子类，最常用的为以下3个:
   * 1、RichSourceFunction(接口类):  带有Rich功能的SourceFunction接口实现类（不可并行）;
   * 2、ParallelSourceFunction(接口):  可并行的SourceFunction接口实现类;
   * 3、RichParallelSourceFunction(抽象类): 可并行的带有rich功能的SourceFunction接口实现类.
   * 此外，还可以实现自定义的SourceFunction.
   */
  def source4DataStream(env: StreamExecutionEnvironment): Unit = {
    /**
     * Flink自带了许多方法以便创建对应的source:
     *
     * 1、fromElements 从元素中获取数据,【不可并行】,底层调用的fromCollection().
     * 2、fromCollection 从集合中获取数据,【不可并行】,底层调用addSource(),传入的是FromElementsFunction类(SourceFunction接口的实现子类)
     * 3、socketTextStream 从socket中获取数据,【不可并行】,底层调用addSource(),传入的是SocketTextStreamFunction类(SourceFunction的实现子类)
     * 4、generateSequence 生成一个序列,【可以并行】,底层调用StatefulSequenceSource类(RichParallelSourceFunction的实现子类)
     * 5、fromParallelCollection 从集合中获取数据,【可以并行】,底层调用addSource(),传入的是FromSplittableorFunction类(FichParallelSourceFunction的实现子类)
     * 6、readTextFile 从文件中获取数据,【可以并行】,底层调用addSource(),传入的是ContinuousFileMonitoringFunction类(RichSourceFunction的实现子类)
     *
     * = = = = = = = = = = = = = = = = =
     * 要使用addSource()实现自定义Souce，关于并行度需要注意的是:
     * 1、直接提供SourceFunction的实现，【不可并行】
     * 2、直接提供RichSourceFunction的实现，【不可并行】
     * 3、直接提供ParallelSourceFunction的实现，【可以并行】
     * 4、直接提供RichParallelSourceFunction的实现，【可以并行】
     */
    import org.apache.flink.api.scala._
    // 基于本地集合的非并行Source.
    //    val source1 = env.fromElements("hadoop", "spark", "flink") // 底层实现fromCollection().
    //    val source2 = env.fromCollection(Array("hadoop", "flink", "hbase"))
    //
    //    source1.setParallelism(1).print()
    //
    //    println(env.getExecutionPlan)
    //    env.execute()

    // 基于本地集合的并行Source.
    //    val source1 = env.generateSequence(1, 10).setParallelism(6)
    //    val source2 = env.fromParallelCollection(new NumberSequenceIterator(1, 10))
    //
    //    source1.print()   // parallelism = 6
    //5> 8
    //7> 1
    //3> 4
    //8> 7
    //4> 2
    //7> 5
    //3> 3
    //4> 6
    //4> 9
    //4> 10
    //    source2.setParallelism(3).print() // parallelism = 7
    //3> 6
    //2> 5
    //7> 3
    //6> 2
    //4> 8
    //5> 1
    //8> 4
    //5> 9
    //6> 10
    //4> 7
    //    println(env.getExecutionPlan)


    // 基于文件的Source.
    //    val localFileSource = env.readTextFile("file:///Users/hiwes/data/input/score.csv","UTF-8")
    //    val hdfsFileSource = env.readTextFile("hdfs://hiwes:8020/inpit/license.txt")
    //    localFileSource.print()
    //    hdfsFileSource.print()

    // 基于网络套接字的Source.
    //    val socketSource = env.socketTextStream("hiwes",9999)
    //    socketSource.print()

    // 自定义实现SourceFunction接口.
    //    ownSourceFunciton(env)

    // 基于Kafka的Source.
    //    kafkaSource(env)

    // 基于MySQL的Source.
    mysqlSource(env)

    env.execute("Flink-DataStream-Test")
  }

  // 3.1自动以实现SourceFunction接口.
  /**
   * 除了预定义的Source外，还可以通过实现SourceFunction来自定义Source，然后通过:
   * env.addSource(sourceFuncion)添加进来.
   *
   * 示例:
   * 自定义数据源，每一秒随机生成一条订单信息(订单ID、用户ID、订单金额、时间戳).
   *
   * @param env
   */
  def ownSourceFunciton(env: StreamExecutionEnvironment): Unit = {
    import org.apache.flink.api.scala._
    //    env.addSource(new MyOwnSource1())
    //      .print()

    //    env.addSource(new MyOwnSource2()).setParallelism(6)
    //      .print()

    env.addSource(new MyOwnSource3).setParallelism(6)
      .print()
  }

  // 实现自定义的SourceFunction创建不可并行的Source.
  class MyOwnSource1 extends SourceFunction[Order] {
    //    private var isRun: Boolean = true // 关闭循环标记.
    //
    //    private final  val random = new Random()
    //    // SourceFunction里面重写方法，主要做数据运行的工作.
    //    override def run(ctx: SourceFunction.SourceContext[Order]): Unit = {
    //      while (isRun) { // 一般使用isRun变量来进行判断是否执行.
    //        val id = UUID.randomUUID().toString()
    //        val userId = random.nextInt(99) + ""
    //        val money = random.nextInt(999)
    //        val time = System.currentTimeMillis()
    //
    //        ctx.collect(Order(id, userId, money, time))
    //        Thread.sleep(1000L)
    //      }
    //    }
    //
    //    // SourceFunction里面重写方法，主要做取消任务.
    //    override def cancel(): Unit = {
    //      this.isRun = false
    //    }

    private var isRun = true

    private final val random = new Random()

    override def run(ctx: SourceFunction.SourceContext[Order]): Unit = {
      while (isRun) {
        // 生成id、userId、money、time
        val id = UUID.randomUUID().toString
        val userId = random.nextInt(99).toString
        val money = random.nextInt(999)
        val time = System.currentTimeMillis()

        ctx.collect(Order(id, userId, money, time))
        Thread.sleep(1000L)

        /**
         * String.valueOf(X)效率最高; 源码其实是调用了Integer.toString(x)。
         * Integer.toString(x)效率中等;
         * ""+x或x+""效率最低.
         */
      }

    }

    override def cancel(): Unit = {
      this.isRun = false
    }
  }

  // 实现自定义的ParallelSourceFunction创建可并行的Source.
  class MyOwnSource2 extends ParallelSourceFunction[Order] {
    private var isRun = true

    override def run(ctx: SourceFunction.SourceContext[Order]): Unit = {
      val random = new Random()
      while (isRun) {
        // 如果不设置无限循环，那么设置了多少并行度，就打印多少条数据.
        val id = UUID.randomUUID().toString
        val userId = random.nextInt(99).toString
        val money = random.nextInt(999)
        val time = System.currentTimeMillis()

        ctx.collect(Order(id, userId, money, time))
        Thread.sleep(1000L)
      }
    }

    override def cancel(): Unit = {
      this.isRun = false
    }
  }

  // 实现自定义的RichParallelSourceFunction创建带Rich的可并行的Source.
  class MyOwnSource3 extends RichParallelSourceFunction[String] {
    /**
     * Rich类型的Source可以比非Rich类型的多出以下几个方法:
     * open():  实例化的时候会执行一次，多个并行度会执行多次;
     * close(): 销毁实例的时候会执行一次，多个并行度会执行多次;
     * getRuntime():  获取当前Runtime对象(底层API).
     */
    override def open(parameters: Configuration): Unit = {
      super.open(parameters)
      println("------open------") // 并行度是几就调用几次.
    }

    override def close(): Unit = {
      super.close()
      println("------close------") // 并行度是几就调用几次.
    }

    override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
      ctx.collect(UUID.randomUUID().toString)
    }

    override def cancel(): Unit = ???
  }

  case class Order(id: String, userId: String, money: Int, time: Long)

  /**
   * 3.2 基于Kafka的Source.
   * Source/Sink: kafka、ActiveMQ、Kinesis Streams、RabbitMQ、NiFi、Pubsub
   * Source: Redis、Twitter Streaming API、Akka
   * Sink: ElasticSearch、HDFS、Flume、Netty、Cassandra
   *
   * @param env
   */
  def kafkaSource(env: StreamExecutionEnvironment): Unit = {
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "hiwes:9092") // 必需
    // kafka0.8版本需要zookeeper.connect属性，现在不需要.
    properties.setProperty("group.id", "flinktest") // 必需
    properties.setProperty("auto.offset.reset", "earliest")
    properties.setProperty("enable.auto.commit", "false") // 是否自动提交偏移量

    val topic = "flinktest"

    /**
     * KafkSoncumser需要知道如何将Kafka的二进制数据转换成Java/Scala对象,常用的反序列化Schema有:
     * 1、SimpleStringSchema: 按字符串方式进行序列化和反序列化.
     * 2、TypeInformationSerializationSchema: 适合读写均是Flink的场景.
     * -------基于Flink的TypeInformation来创建Schema，这种Flink-specific的反序列化会比其他通用的序列化方式性能更高.
     * 3、JSONDeserializationSchema: 可以吧序列化后的json反序列化为ObjectNode，可通过Object.get("field").as(Int/String/...)()来访问指定字段.
     */
    val schema = new SimpleStringSchema()
    //    val schema = new TypeInformationSerializationSchema[]()
    //    val schema = new JSONKeyValueDeserializationSchema()

    val kafkaConsumer: FlinkKafkaConsumer010[String] = new FlinkKafkaConsumer010[String](
      topic, schema, properties)

    /**
     * 指定source从哪开始消费:
     * kafkaConsumer.setStartFromEarliest() // 从头开始消费.
     * kafkaConsumer.setStartFromTimestamp(System.currentTimeMillis())  // 从指定的时间戳开始消费.
     * kafkaConsumer.setStartFromGroupOffsets() // 从group中记录的offset开始消费. [ 默认 ]
     * kafkaConsumer.setStartFromLatest() // 从最新开始消费.
     *
     * 以及: 指定每个从某topic的某个分区的某个offset开始消费.
     * val offsets = new mutable.HashMap[KafkaTopicPartition,Long]()
     * offsets.put(new KafkaTopicPartition(topic, 0), 0L)
     * offsets.put(new KafkaTopicPartition(topic, 1), 0L)
     * offsets.put(new KafkaTopicPartition(topic, 2), 0L)
     *
     * kafkaConsumer.setStartFromSpecificOffsets(offsets)
     */
    kafkaConsumer.setStartFromEarliest()

    import org.apache.flink.api.scala._
    val kafkaSource = env.addSource(kafkaConsumer)
    kafkaSource.print()
  }

  /**
   * 3.2.2以下为Topic和partition的动态发现配置.
   * 实际的生产环境中，可能有以下需求，如:
   * 1、flink在不重启作业的情况下，自动感知新的topic;
   * 2、flink在不重启作业的情况下，自动感知新扩容的partition.
   * 针对以上情况，首先需要设置flink.partition-discovery.interval-millis为【非负值】.
   * 表示: 开启动态发现的开关，以及设置的时间间隔.
   * 至此,flinkkafkaConsumer内部会启动一个单独的线程定期去kafka获取新的meta信息.
   * 所以针对以上情况:
   * 1、topic的描述可以传一个正则表达式的pattern，从而获取最新topic列表.
   * 2、设置动态参数，定期获取kafka最新meta消息，为保证数据准确性，新发现的partition从最早的位置开始读取.
   */
  def kafkaSource2(env: StreamExecutionEnvironment): Unit = {
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "hiwes:9092")
    properties.setProperty("group.id", "flink-test")
    properties.setProperty("auto.offset.reset", "earliest")
    properties.setProperty("enable.auto.commit", "false")
    properties.setProperty("flink.partition-discovery.interval-millis", "30000")

    val kafkaConsumer = new FlinkKafkaConsumer010[String](
      java.util.regex.Pattern.compile("flinktest-[0-9]"), // 使用正则表达式
      new SimpleStringSchema(),
      properties)

    kafkaConsumer.setStartFromEarliest()

    import org.apache.flink.api.scala._
    env.addSource(kafkaConsumer)
      .print()
  }


  /**
   * 3.3 基于MySQL的Source.
   *
   * @param env
   */
  def mysqlSource(env: StreamExecutionEnvironment): Unit = {
    import org.apache.flink.api.scala._
    val mysqlSource = env.addSource(new MyMySQLSource())
    mysqlSource.print()
  }

  class MyMySQLSource extends RichSourceFunction[UserInfo] {
    private var connection: Connection = null
    private var ps: PreparedStatement = null

    // 创建实例执行一次，适合用来做数据库连接.
    override def open(parameters: Configuration): Unit = {
      super.open(parameters)
      Class.forName("com.mysql.jdbc.Driver")

      val url = "jdbc:mysql://hiwes:3306/test?useUnicode=true&characterEncoding=utf-8&useSSL=false"
      this.connection = DriverManager.getConnection(url, "root", "root")

      this.ps = connection.prepareStatement("select * from user")
    }

    // 销毁实例执行一次，适合用来做关闭连接.
    override def close(): Unit = {
      super.close()

      if (this.ps != null) this.ps.close()
      if (this.connection != null) this.connection.close()
    }

    override def run(ctx: SourceFunction.SourceContext[UserInfo]): Unit = {
      val resultSet = ps.executeQuery()
      while (resultSet.next()) {
        val id = resultSet.getInt("id")
        val username = resultSet.getString("username")
        val password = resultSet.getString("password")
        val name = resultSet.getString("name")

        ctx.collect(UserInfo(id, username, password, name))
      }
    }

    override def cancel(): Unit = {
      println("-------任务取消.-------")
    }
  }

  case class UserInfo(id: Int, username: String, password: String, name: String)

  // 4.DataStream的Transformation.
  /**
   * 其实和DataSet的算子很类似，但是DataSet的groupBy需要换成KeyBy.
   *
   * keyBy  根据指定的key进行分组，逻辑上把DataStream分为若干不相交的分区，key一样的event划分到相同partition.
   * -------内部采用hash分区实现. DataStream -----> KeyedStream
   * reduce 分组后keyedStream数据进行聚合操作. KeyedStream -----> DataStream
   * aggregations 和DataSet类似的聚合操作.
   * union  connect之后生成ConnectedStreams，对两个流的数据应用不同的处理方法，且双流之间可共享状态（包括计数）
   * -------第一个流中的输入会影响第二个流时，会很有用. 不会去重.
   * connect  合并流.连接两个保持类型的数据流，被放在同一个流中，内部依然保持各自的数据和形式不变.
   * -------DataStream -----> ConnectedStream.
   * split  拆分流.切分流DataStream -----> SplitStream
   * select 获取分流后对应的数据，搭配split使用. SplitStream -----> DataStream
   *
   * @param env
   */
  def transformations4DataStream(env: StreamExecutionEnvironment): Unit = {
    import org.apache.flink.api.scala._

    env.setParallelism(1)

    // keyBy+sum.
    //    val source = env.fromElements(
    //      ("篮球", 1),
    //      ("篮球", 2),
    //      ("篮球", 3),
    //      ("足球", 3),
    //      ("足球", 2),
    //      ("足球", 3)
    //    )
    //    source.keyBy(0).sum(1)
    //      .print()

    // reduce+fold+aggregations，分组后对keyedStream数据进行各种聚合操作，类似SQL操作.
    //    val socketStream = env.socketTextStream("hiwes", 9999)
    //    socketStream.map(x => (x.split(",")(0), x.split(",")(1).toInt))
    //      .keyBy(0)
    //      .reduce((x,y) => (x._1,x._2+y._2))
    //      .print()

    // union
    //    val source1 = env.fromElements("hadoop", "hive", "flink")
    //    val source2 = env.fromElements("hadoop", "hive", "spark")
    //
    //    val source3 = env.fromElements(1, 2, 3)
    //
    //    val unionStream = source1.union(source2)
    //    // 因为类型不一致，所以无法合并
    //    // val unionStream2 = source1.union(source3)
    //    unionStream.print()

    // connect
    //    val source1 = env.addSource(new IncreaseByOneSource())
    //    val source2 = env.addSource(new IncreaseByOneSource())
    //
    //    val connectedStream = source1.connect(source2)
    //    connectedStream.map(new CoMapFunction[Int, Int, String] {
    //      override def map1(value: Int): String = "map1:" + value
    //
    //      override def map2(value: Int): String = "map2:" + value
    //    }).print()

    // split & select
    val source = env.fromElements(1, 2, 3, 4, 5, 6)
    val splitedStream = source.split(new OutputSelector[Int] {
      override def select(value: Int): lang.Iterable[String] = {
        val flagList = new util.ArrayList[String]()
        if (value % 2 == 0) flagList.add("even")
        else {
          flagList.add("odd")
          flagList.add("jishu")
        }
        flagList
      }
    })

    val evenStream = splitedStream.select("even") // 偶数
    evenStream.print("even>>>")

    val oddStream = splitedStream.select("odd") // 奇数
    oddStream.print("odd>>>")

    val jishuStream = splitedStream.select("jishu") // 另一个标记的奇数
    jishuStream.print("jishu>>>")

    env.execute()

  }

  class IncreaseByOneSource extends SourceFunction[Int] {
    private var isRun = true
    private final val random = new Random()

    override def run(ctx: SourceFunction.SourceContext[Int]): Unit = {
      while (isRun) {
        ctx.collect(random.nextInt(999))
        Thread.sleep(1000L)
      }
    }

    override def cancel(): Unit = {
      this.isRun = false
    }
  }


  // 5.DataStream的Sink.
  /**
   * 常见的Sink:
   * 1、基于本地集合的Sink: print、printToErr
   * 2、基于文件的Sink
   * 3、自定义的Sink
   * 4、基于kafka的Sink
   *
   * @param env
   */
  def sinks4DataStream(env: StreamExecutionEnvironment): Unit = {
    import org.apache.flink.api.scala._

    // 基于本地集合的Sink.
    //    val source = env.fromElements(1, 2, 3, 4, 5, 6)
    //    source.print()
    //    source.print("前缀打印>>>")
    //    source.printToErr("StdErr前缀打印>>>")

    // 基于文件的Sink.
    //    val source = env.fromElements(
    //      (19, "潇潇", 170.50),
    //      (11, "甜甜", 168.8),
    //      (16, "刚刚", 178.8),
    //      (19, "蛋蛋", 179.99)
    //    )
    //    // 写出到本地文件
    //    source.writeAsText("file:///Users/hiwes/data/output/SinktoLocalFile.txt", FileSystem.WriteMode.OVERWRITE)
    //    // 写出到HDFS
    //    source.writeAsText("hdfs://hiwes:8020/output/SinkToHDFS.txt", FileSystem.WriteMode.OVERWRITE).setParallelism(1)
    //    // 写出到CSV
    //    source.writeAsCsv("file:///Users/hiwes/data/output/SinktoCSV.csv", FileSystem.WriteMode.OVERWRITE,
    //      "\n", ",").setParallelism(1)

    /**
     * 综上，writeAsText已经弃用，所以使用StreamingFileSink.
     * 这个连接器，提供了一个Sink来将分区文件写入支持Flink FileSystem接口的文件系统中.
     */
    // 基于连接器StreamingFileSink.
    //    env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE)
    //    env.setStateBackend(new FsStateBackend("file:///Users/hiwes/data/output/checkpoint"))
    //    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500)
    //
    //    val sockectStream = env.socketTextStream("hiwes", 9999)
    //    val sink = StreamingFileSink.forRowFormat(
    //      new Path("file:///Users/hiwes/data/output/sink3"),
    //      new SimpleStringEncoder[String]("UTF-8"))
    //      .withBucketAssigner(new BasePathBucketAssigner[String]())
    //      .withRollingPolicy(OnCheckpointRollingPolicy.build())
    //      .build()
    //    sockectStream.addSink(sink)

    // Sink到Kafka.
    //    val source = env.fromElements(
    //      "Sink to Kafka Test 1",
    //      "Sink to Kafka Test 2",
    //      "Sink to Kafka Test 3",
    //      "Sink to Kafka Test 4",
    //      "Sink to Kafka Test 5",
    //      "Sink to Kafka Test 6"
    //    )
    //
    //    val properties = new Properties()
    //    properties.setProperty("bootstrap.servers", "hiwes:9092")
    //    properties.setProperty("group.id", "flink-test")
    //    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    //    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    //    properties.setProperty("auto.offset.reset", "latest") //偏移量自动重置
    //
    //    //    val kafkaSink = new FlinkKafkaProducer010[String](
    //    //      "flinktest",
    //    //      new MyKafkaSerializationSchema(),
    //    //      properties
    //    //    )
    //    val kafkaSink = new FlinkKafkaProducer010[String](
    //      "flinktest",
    //      new SimpleStringSchema(),
    //      properties
    //    )
    //    source.addSink(kafkaSink)

    // sink到mysql.
    //    val source = env.fromElements(UserInfo(9, "xiaoxiao", "123456", "潇潇"))
    //    source.addSink(new MyMySQLSink())

    // sink到redis.
    val socketStream = env.socketTextStream("hiwes", 9999)

    val map = socketStream.map(x => (x.split(" ")(0), x.split(" ")(1)))

    val redisConf = new FlinkJedisPoolConfig.Builder().setHost("hiwes").setPort(6379).build()
    map.addSink(new RedisSink[(String, String)](redisConf, new MyRedisMapper()))

    env.execute()

  }

  class MyRedisMapper extends RedisMapper[(String, String)] {
    override def getCommandDescription: RedisCommandDescription = new RedisCommandDescription(RedisCommand.LPUSH)

    override def getKeyFromData(t: (String, String)): String = t._1

    override def getValueFromData(t: (String, String)): String = t._2
  }


  class MyKafkaSerializationSchema extends KafkaSerializationSchema[String] {
    val topic = "flinktest"

    override def serialize(t: String, aLong: lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
      new ProducerRecord[Array[Byte], Array[Byte]](topic, t.getBytes())
    }
  }

  class MyMySQLSink extends RichSinkFunction[UserInfo] {
    private var connection: Connection = null
    private var ps: PreparedStatement = null

    override def invoke(value: UserInfo, context: SinkFunction.Context[_]): Unit = {
      this.ps.setInt(1, value.id)
      this.ps.setString(2, value.username)
      this.ps.setString(3, value.password)
      this.ps.setString(4, value.name)
      this.ps.execute()
    }

    override def open(parameters: Configuration): Unit = {
      super.open(parameters)
      val url = "jdbc:mysql://hiwes:3306/test?useUnicode=true&characterEncoding=utf-8&useSSL=false"
      Class.forName("com.mysql.jdbc.Driver")
      this.connection = DriverManager.getConnection(url, "root", "root")
      this.ps = connection.prepareStatement("insert into user values(?,?,?,?);")
    }

    override def close(): Unit = {
      super.close()
      if (this.ps != null) this.ps.close()
      if (this.connection != null) this.connection.close()

    }
  }

}
