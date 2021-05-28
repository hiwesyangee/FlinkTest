package com.hiwes.flink.Zinterview.highlevel_4

import java.sql.{Connection, DriverManager, PreparedStatement, SQLException}
import java.text.SimpleDateFormat
import java.{lang, util}
import java.util.{Date, Properties}
import java.util.concurrent.{CompletableFuture, TimeUnit}
import java.util.function.{Consumer, Supplier}

import org.apache.flink.api.common.JobID
import org.apache.flink.api.common.functions.{MapFunction, RichReduceFunction}
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, MapStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.core.fs.CloseableRegistry
import org.apache.flink.metrics.MetricGroup
import org.apache.flink.runtime.execution.Environment
import org.apache.flink.runtime.query.TaskKvStateRegistry
import org.apache.flink.runtime.state.{AbstractKeyedStateBackend, CheckpointStorage, CompletedCheckpointStorageLocation, FunctionInitializationContext, FunctionSnapshotContext, KeyGroupRange, KeyedStateHandle, OperatorStateBackend, OperatorStateHandle, StateBackend}
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.runtime.state.ttl.TtlTimeProvider
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.datastream.AsyncDataStream
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, KeyedProcessFunction}
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.async.{ResultFuture, RichAsyncFunction}
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment, WindowedStream}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, GlobalWindows, ProcessingTimeSessionWindows, SlidingEventTimeWindows, TumblingEventTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala.{BatchTableEnvironment, StreamTableEnvironment}
import org.apache.flink.util.Collector

import scala.util.Random

/**
 * About Flink High Level.
 *
 * @by hiwes since 2021/02/20
 */
object FlinkHighLevel1 {

  private final val sdf = new SimpleDateFormat("mm:ss.SSS")

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val senv = StreamExecutionEnvironment.getExecutionEnvironment

    // 1.SQL&Table API.

    // 2.FlinkSQL案例.
    flinkSQLTest4Batch(env)
    flinkSQLTest4Stream(senv)

    // 3.Flink的Window操作.
    flinkWindowTest(senv)

    // 4.Flink的WaterMark.
    waterMarkTest(senv)
    allowedLatenessTest(senv)
    sideOutputDataTest(senv)

    // 5.Flink的异步IO.
    asyncIOTest(senv)

    // 6.Flink的状态State管理.
    stateCheckpointTest(senv)

    // 7.Flink的容错机制.
    checkpointTest(senv)
    checkpointTest2(senv)
    checkpointTest3(senv)
    checkpointTestLast(senv)
    savePointTest(senv)

    // 8.Flink的End-to-End Exactly-Once语义.
    end2EndExactlyOnceTest(senv)

    // 9.Flink的ProcessFunction API.
    systemMonitoringTest(senv)
  }

  // 1.SQL&Table API.
  /**
   * Flink SQL是以Flink实时计算为简化计算模型，降低用户使用实时计算门槛设计的一套符合标准SQL语义的开发语言.
   * 【主要是流和批的统一，Flink底层Runtime本身就是一个流和批统一的引擎，而SQL可以做到API层的流和批统一】
   *
   * Table API & SQL是一种关系型API，可以操作SQL一样操作数据，而不需要写Flink代码。好处也很明显:
   * 1、关系型API是声明式的;
   * 2、能被有效的优化并高效的执行;
   * 3、所有人都知道SQL.
   * Flink本身是批和流统一的处理框架，Table API & SQL是批和流统一的上层处理API.
   * FlinkSQL其实本身比Flink DataSet/DataStream API还耗时间.
   * 所以主流还是使用SparkSQL完成分布式SQL的开发。
   */

  // 2.FlinkSQL案例.
  /**
   * 需要导入的包中:
   * flink-table-planner: planner计划期，是table API最重要的部分，提供了运行时环境和生成程序执行计划的planner，翻译SQL为Flink代码;
   * flink-table-api-scala-bridge: bridge桥接器，主要负责table API和DataSet/DataStream的连接支持，按语言分为java和scala.
   * 一般生产环境lib目录下默认有planner，只需要bridge即可.
   * 以下为批处理案例.
   */
  def flinkSQLTest4Batch(env: ExecutionEnvironment): Unit = {
    val tEnv = BatchTableEnvironment.create(env) // 重点.带入BatchTableExvironment

    import org.apache.flink.api.scala._
    val studentDataSource = env.fromElements(
      Student2("潇潇", 22, "male"),
      Student2("甜甜", 11, "female"),
      Student2("蛋蛋", 10, "male"),
      Student2("刚刚", 9, "male"),
      Student2("妞妞", 16, "female")
    )

    //    val table:Table = tEnv.fromDataSet(studentDataSource,"name,age,sex")
    val table: Table = tEnv.fromDataSet(studentDataSource)
    val tableResult: Table = table.where("age >= 10").groupBy("sex").select("sex, max(age) as MaxAge")
    val queryResultDataSet: DataSet[QueryResult] = tEnv.toDataSet(tableResult)
    queryResultDataSet.print()

    tEnv.createTemporaryView("student", studentDataSource)
    val tableResult2: Table = tEnv.sqlQuery("Select sex, Max(age) as maxAge from student where age >= 10 group by sex")
    val queryResultDataSet2: DataSet[QueryResult] = tEnv.toDataSet(tableResult2)
    queryResultDataSet2.print()
  }

  case class QueryResult(sex: String, maxAge: Int)

  case class Student2(name: String, age: Int, sex: String)

  // 2.2流处理案例
  def flinkSQLTest4Stream(senv: StreamExecutionEnvironment): Unit = {
    import org.apache.flink.api.scala._
    val tEnv = StreamTableEnvironment.create(senv) // 重点.带入StreamTableExvironment

    val socketStream = senv.socketTextStream("hiwes", 9999)
    val queryDataStream = socketStream.map(x => QueryFieldOfID(x.toInt))

    val studentDataStreamSource: DataStream[Student3] = senv.fromElements(
      Student3(1, "潇潇", 11, "female"),
      Student3(2, "刚刚", 10, "male"),
      Student3(3, "蛋蛋", 9, "male"),
      Student3(4, "林俊杰", 8, "male"),
      Student3(5, "林志玲", 9, "female"),
      Student3(6, "刘德华", 8, "male"),
      Student3(7, "张靓颖", 7, "female"),
      Student3(8, "梁朝伟", 11, "male"),
      Student3(9, "欧阳娜娜", 16, "female"),
      Student3(10, "张艺兴", 10, "male"),
      Student3(11, "迪丽热巴", 11, "female")
    )

    // 将两个DataStream注册为Table对象.
    val queryFieldTable: Table = tEnv.fromDataStream(queryDataStream)
    val studentTable: Table = tEnv.fromDataStream(studentDataStreamSource)

    // Table API形式.
    val tableResult1: Table = queryFieldTable.leftOuterJoin(studentTable, "id2 = id").select("id2 as idn,name, age,sex")
    val result1: DataStream[(Boolean, Student3)] = tEnv.toRetractStream(tableResult1)
    result1.print()

    // SQL形式.
    tEnv.createTemporaryView("query_id", queryDataStream)
    tEnv.createTemporaryView("student", studentDataStreamSource)

    val tableResult2 = tEnv.sqlQuery("select id2 as idn,name,age,sex from query_id as q left outer join student as stu on stu.id2 = q.id")
    val result2: DataStream[(Boolean, Student3)] = tEnv.toRetractStream(tableResult2)
    result2.print()

    senv.execute()

  }

  case class Student3(id2: Int, name: String, age: Int, sex: String)

  case class QueryFieldOfID(id: Int)

  // 3.Flink的Window操作.
  /**
   * Flink认为Batch是Streaming的一个特例，所以Flink底层引擎是一个流式引擎，只是在上层实现了流处理和批处理.
   * Window就是从Streaming到Batch的一个桥梁，Flink提供了非常完善的窗口机制.
   * Window可以分为2类:
   * 1、CountWindow: 按指定的数据条数生成一个Window，与时间无关.
   * -------滚动计数窗口，每隔n条数据，统计前n条数据;
   * -------滑动计数窗口，每隔n条数据，统计n条数据;
   * 2、TimeWindow:  按时间生成Window.
   * -------滚动时间窗口，每隔n时间，统计前n时间范围内的数据，窗口长度n，滑动距离n;
   * -------滑动时间窗口，每隔n时间，统计前m时间范围内的数据，窗口长度n，滑动距离m;
   * -------会话窗口，按照会话规定的窗口.
   *
   * 【滚动窗口和滑动窗口】
   * 1、滚动窗口————Tumbling Window.
   * 需求: 统计每1分钟内通过的汽车数量.
   * 流式连续而无界的，确定了数据的边界，从无界的流数据中取出一部分有边界的数据子集进行计算.
   * 按时间进行统计就是时间窗口，按数量进行统计就是计数窗口.
   * 且每一部分相对独立，数据在其中一份，就不会在另外一份.
   * 窗口长度:  指窗口的范围，或者说数据边界的范围.
   * 滚动窗口中，窗口长度 == 滑动距离.
   * 2、滑动窗口————Sliding Window.
   * 需求: 每1分钟统计之前2分钟内通过的汽车数量.
   * 从中可以看出，滑动窗口也就是滑动距离，不等于窗口长度.
   * 滑动窗口中，窗口长度 != 滑动距离.此时如果出现:
   * 滑动距离 > 窗口长度. 会漏掉数据，比如每5分钟，统计之前1分钟的数据;
   * 滑动距离 < 窗口长度. 会重复处理数据，比如每1分钟，统计之前2分钟的数据;
   * 滑动距离 = 窗口长度. 不漏也不重复，也就是滚动窗口.
   *
   * 【Flink流处理时间方式Time】
   * 对于时间窗口来说，最主要的就是时间，那么这个时间是怎么定义的呢？Flink针对时间有3种类型:
   * 1、EventTime  事件时间. 事件发生的时间.
   * 2、IngestionTime  摄入时间. 某个Flink节点的SourceOperator接收到数据的时间.如: 某个Source消费到kafka的时间.
   * 3、ProcessingTime 处理时间. 某个Flink节点执行某个Operator的时间.如: timeWindow接收到数据的时间.
   * 在Flink的流式处理中，绝大部分业务都会使用EventTime,一般在EventTime不能使用的时候，才被迫使用ProcessingTime或者IngestionTime.
   * > env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
   *
   * 【窗口的范围】
   * 窗口的判断按毫秒为单位,如果窗口长度为5000ms,
   * 窗口的开始: start = 0
   * 窗口的结束: start + 5000 - 1  = 4999
   * 窗口不会一直存在，当达到某些条件后，窗口就会执行触发计算+关闭窗口的动作。
   * 窗口的关闭和触发有两个步骤: 触发，对窗口内的数据进行计算; 关闭，数据就无法再进入窗口了.
   * 其中: 开始时间和结束时间二者结合，决定了数据属于哪个窗口.数据的时间要满足:
   * ---1. >= 开始时间.
   * ---2. <= 结束时间.
   * 如4999就属于这个窗口，5000就不属于这个窗口.
   * 如果要使用【处理时间ProcessingTime或摄入时间IngestionTime】.
   * ------ 窗口会按照系统时间进行判断。如果当前系统时间 >= 窗口结束时间，则窗口被关闭并触发计算.
   * ------ 比如: 0——5000的窗口，系统时间走到了 >= 4999就会触发窗口计算和关闭.
   * 如果要使用【事件时间EventTime】.
   * ------ 当新进入一条数据的时候，其事件时间 >= 某个窗口的结束时间,则窗口被关闭并触发计算.
   * ------ 比如: A窗口是0——5000，B窗口是5000——10000,那么:
   * ------ 当数据事件时间 >= 4999的数据进来之后，会导致窗口A被关闭和触发计算.
   * 如果要使用【水印WaterMark】.
   * ------ 当新进入的一条数据，其水印时间 >= 某个窗口的结束时间，窗口被关闭并触发计算.
   * 总结:
   * --- 处理时间通过当前系统时间决定窗口触发和关闭，较为稳定，比如每5s窗口就每隔5s触发一次.
   * --- 事件时间通过进入flink的数据，所带的事件时间来决定是否关闭窗口，只要数据不进入此flink，就一直不会关闭。不稳定，取决于数据.
   * --- 水印时间基于数据的事件时间，一样开闭不稳定，取决于数据是否到来和事件时间的多少.
   *
   * @param senv
   */
  def flinkWindowTest(senv: StreamExecutionEnvironment): Unit = {
    /**
     * 给数据加窗口的2种方法,window()和windowAll()
     * window()  仅针对keyBy后的流进行使用，对分流后的每个子流加窗口.
     * windowAll()   使用keyBy和未使用KeyBy的分流的流都可使用，会忽略是否进行了keyBy.
     * 区别:
     * 使用keyBy分流后的流如果调用windowAll，相当于未分流的效果，Flink会忽略分流后的各个子流，且将全量数据一起进行窗口计算.
     * 未使用keyBy分流的数据只能调用windowAll()，无法调用window().
     * 两个方法都需要传入一个WindowAssigner对象.指的就是具体类型是什么，是时间窗口还是计数窗口还是会话窗口.
     * 一般都是使用这个抽象类的子类，包括:
     * 1、DynamicEventTimeSessionWindows  动态事件时间会话窗口.
     * 2、DynamicProcessingTimeSessionWindows 动态处理时间会话窗口.
     * 【常用】3、EventTimeSessionWindows 事件时间会话窗口.
     * 【常用】4、GlobalWindows 全局窗口（计数窗口使用的这个实现）
     * 5、MergingWindowAssigner 合并窗口（一般不直接使用）
     * 【常用】6、ProcessingTimeSessionWindows  处理时间会话窗口.
     * 【常用】 7、SlidingEventTimeWindows 滑动事件时间窗口.
     * 【常用】8、TumblingEventTimeWindows  滚动事件时间窗口.
     * 【常用】9、TumblingProcessingTimeWindows 滚动处理时间窗口.
     *
     * 针对未分流的流对象————windowAll:
     * countWindowAll(窗口长度).  创建滚动计数窗口，底层创建GlobalWindows对象，调用windowAll()用来创建滚动计数窗口.
     * --- windowAll(GlobalWindows.create()).trigger(PurgingTrigger.of(COuntTrigger.of(size)))
     * countWindowAll(窗口长度,滑动距离). 创建滑动计数窗口，底层创建GlobalWindows对象，调用windowAll()用来创建滑动计数窗口.
     * --- windowAll(GlobalWindows.create()).evictor(CountEvictor.of(size)).trigger(COuntTrigger.of(slide))
     * timeWindowAll(窗口长度). 创建滚动时间窗口，底层会创建具体的实例化对象.
     * --- 会自动判断，使用事件时间就用TumblingEventTimeWindows; 使用处理时间就用TrumblingProcessingTimeWindows.
     *
     * 针对分流后的流对象————window:
     * timeWindow()
     * countWindow()
     * timeWindowAll()
     * countWindowAll()
     *
     * 【注意】如果设置为了事件时间EventTime，一定要告知Flink，在被处理的数据中事件时间是谁.
     * assignTimestampAndWaterMarks.  告知事件时间在被处理的数据中是哪个字段.
     * 否则会报错:
     * Caused by: java.lang.RuntimeException: Record has Long.MIN_VALUE timestamp (= no timestamp marker).
     * Is the time characteristic set to 'ProcessingTime', or did you forget to call 'DataStream.assignTimestampsAndWatermarks(...)'?
     *
     */
    // Time Window
    //    timeWindowTest(senv) // 滚动时间窗口.
    //    timeWindowTest2(senv) // 滑动事件窗口.

    // Count Window
    //    countWindowTest(senv) // 滚动计数窗口
    //    countWindowTest2(senv) // 滑动计数窗口

    // Session Window
    //    sessionWindowTest(senv)

    // 自定义Window聚合计算
    customWindowTest(senv)

  }

  // 3.1.1 滚动时间窗口，无重叠数据.
  // 自定义一个Source,每1秒产生一个k,v，对数据加窗口1统计数字总和，窗口2对按key分组后的数据统计每个key对应的数据总和.
  def timeWindowTest(senv: StreamExecutionEnvironment): Unit = {
    import org.apache.flink.api.scala._

    // 自定义Source，每秒产生一个k-v
    val randomIntSource = senv.addSource(new GenerateRandomNumEverySecond())

    // 实现窗口1
    val sumOfWindowAll = randomIntSource.timeWindowAll(Time.seconds(5)).sum(1)

    // 实现窗口2
    val sumOfWindow = randomIntSource.keyBy(0).timeWindow(Time.seconds(5)).sum(1)

    sumOfWindowAll.print("Sum all >>>")
    sumOfWindow.print("Sum each key >>>")

    senv.execute()
  }

  class GenerateRandomNumEverySecond extends SourceFunction[(String, Int)] {
    private var isRun = true
    private final val random = new Random()
    private final val keyList: List[String] = List("hadoop", "spark", "flink")

    override def run(ctx: SourceFunction.SourceContext[(String, Int)]): Unit = {
      while (this.isRun) {
        val key = keyList(random.nextInt(3))
        val value = (key, random.nextInt(99))
        ctx.collect(value)
        println("------: " + value)
        Thread.sleep(1000L)
      }
    }

    override def cancel(): Unit = {
      this.isRun = false
    }
  }

  // 3.1.2 滑动时间窗口，有重叠数据.
  /**
   *
   * timwWindow(size, slide)
   * slide > size,滑动距离大于窗口长度，会有数据丢失;
   * slide < size,滑动距离小于窗口长度，会有数据重复;
   * slide = size,滑动距离等于窗口长度，等于滚动窗口,不丢失不重复.
   */
  def timeWindowTest2(senv: StreamExecutionEnvironment): Unit = {
    import org.apache.flink.api.scala._

    val source = senv.addSource(new GenerateRandomNumEverySecond())

    // （窗口长度size,滑动距离slide）每隔slide,统计前size的数据.
    val sumAll = source.timeWindowAll(Time.seconds(2), Time.seconds(5)).sum(1)
    val sumEachKey = source.keyBy(0).timeWindow(Time.seconds(10), Time.seconds(5)).sum(1)

    sumAll.print("sum all >>> ")
    sumEachKey.print("sum each key >>> ")

    senv.execute()
  }

  // 3.2.1 滚动计数窗口.无重叠数据.
  def countWindowTest(senv: StreamExecutionEnvironment): Unit = {
    import org.apache.flink.api.scala._
    val source = senv.addSource(new GenerateRandomNumEverySecond())

    // 统计全部数据，每隔5条.
    source.countWindowAll(5).sum(1).print("Sum all >>>")

    source.keyBy(0).countWindow(5).sum(1).print("Sum each key >>> ")

    senv.execute()
  }

  // 3.2.2 滑动计数窗口,有重叠数据.
  def countWindowTest2(senv: StreamExecutionEnvironment): Unit = {
    import org.apache.flink.api.scala._

    val source = senv.addSource(new GenerateRandomNumEverySecond())

    source.countWindowAll(10, 5).sum(1).print("Sum all >>>")

    source.keyBy(0).countWindow(10, 5).sum(1).print("Sum each key >>> ")

    senv.execute()
  }

  /**
   * 3.3 会话窗口. 是基于时间的窗口, 针对时间的窗口，分为3类:
   * 1、TumblingEventTimeWindows.  滑动事件时间窗口
   * 2、SlidingEventTimeWindows.   滚动事件时间窗口
   * 3、EventTimeSessionWindows.   事件时间会话窗口.会话窗口的窗口大小，由数据本身决定.比如:
   * key,00
   * key,03
   * key,05
   * key,12
   * key,15
   * key,24
   * key,30
   * key,42
   * 那么,如果session window的时间间隔为6s，则上面的数据会被分为以下几个窗口:
   * --------------------
   * key,00
   * key,03
   * key,05
   * --------------------
   * key,12
   * key,15
   * --------------------
   * key,24
   * --------------------
   * key,30
   * --------------------
   * key,42
   * --------------------
   * 注意 ——————> 当相邻两条数据相差 >= 6s的时候，会触发窗口
   * 窗口大小 = （第一条数据的时间, 第一个与相邻数据相差 >= 6的时间 + 6）
   * 意思就是说，窗口内包含的数据是“活跃的”.
   * 举例:
   * 用户点击行为，认为30s间隔用户没有操作则认为是不活跃的.
   * 通过session window,定义一个30s的gap，此时每个窗口内的数据，都是用户在活跃期间的数据
   * 超过30s没有任何操作，认为用户不活跃，有可能下线.
   */
  def sessionWindowTest(senv: StreamExecutionEnvironment): Unit = {
    import org.apache.flink.api.scala._
    val source = senv.addSource(new GenerateRandomNumEverySecond2())

    //    source.windowAll(ProcessingTimeSessionWindows.withGap(Time.seconds(5))).sum(1)
    //      .print(sdf.format(new Date()) + "| sum print >>> ")

    source.keyBy(0).window(ProcessingTimeSessionWindows.withGap(Time.seconds(5))).sum(1)
      .print(sdf.format(new Date()) + "| each key print >>> ")

    //     * 1、DynamicEventTimeSessionWindows  动态事件时间会话窗口.
    //     * 2、DynamicProcessingTimeSessionWindows 动态处理时间会话窗口.
    //     * 【常用】3、EventTimeSessionWindows 事件时间会话窗口.
    //     * 【常用】4、GlobalWindows 全局窗口（计数窗口使用的这个实现）
    //     * 5、MergingWindowAssigner 合并窗口（一般不直接使用）
    //     * 【常用】6、ProcessingTimeSessionWindows  处理时间会话窗口.
    //     * 【常用】 7、SlidingEventTimeWindows 滑动事件时间窗口.
    //     * 【常用】8、TumblingEventTimeWindows  滚动事件时间窗口.
    //     * 【常用】9、TumblingProcessingTimeWindows 滚动处理时间窗口.

    senv.execute()
  }


  class GenerateRandomNumEverySecond2 extends SourceFunction[(String, Int)] {
    private var isRun = true
    private final val random = new Random()
    private final val keyList = List("hadoop"
      //      , "spark", "flink")
    )

    override def run(ctx: SourceFunction.SourceContext[(String, Int)]): Unit = {
      while (this.isRun) {
        // val key = keyList(random.nextInt(3))
        val key = keyList(0)
        val value = (key, random.nextInt(9))
        ctx.collect(value)
        var sleepTime = 5000L // 模拟的是点击间隔时间.
        while (sleepTime == 5000L) {
          sleepTime = random.nextInt(7) * 1000L
        }
        println(sdf.format(new Date()) + ": will sleep " + sleepTime + " ms --- : " + value)
        Thread.sleep(sleepTime)
      }
    }

    override def cancel(): Unit = {
      this.isRun = false
    }
  }

  /**
   * 3.4 自定义窗口聚合计算.
   * 自定义实现聚合，需要用到apply().
   * apply 进行一些自定义处理，通过匿名内部类方法实现, 当有一些复杂计算的时候使用.
   * apply(WindowFunction接口的实例对象)
   *
   * 以下示例为使用apply进行单词统计.
   */
  def customWindowTest(senv: StreamExecutionEnvironment): Unit = {
    import org.apache.flink.api.scala._

    val socketTextStream = senv.socketTextStream("hiwes", 9999)

    socketTextStream.flatMap(_.toLowerCase().split(" "))
      .filter(_.nonEmpty)
      .map((_, 1))
      .keyBy(0)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
      // apply(new WindowWunction(key, window, input, out))
      .apply(new WindowFunction[(String, Int), (String, Int), Tuple, TimeWindow] {
        override def apply(tuple: Tuple, window: TimeWindow, input: Iterable[(String, Int)], out: Collector[(String, Int)]): Unit = {
          var sum = 0
          var key: String = null
          for (wordWithOne <- input) {
            sum += wordWithOne._2
            key = wordWithOne._1
          }
          out.collect((key, sum))
        }
      })
      .print()

    senv.execute()
  }

  /**
   * 4.Flink的WaterMark.
   * 原因: 流处理从事件产生，到流经source，再到operator，中间有一个过程和时间。大部分情况下，到operator的数据都是按照事件产生的时间顺序来的，
   * 但是也不排除由于网络、背压等原因，导致乱序的产生。所谓乱序，就是指Flink接收到的事件的先后顺序不是严格按照事件=的EventTime顺序排列.
   * 一个本该10:00进入窗口的数据，因为网络延迟，进入窗口的时间为10:02，延迟了2秒，那么就会导致数据统计不正确.
   * 如果按处理时间来计算，这个窗口会在系统时间 >= 10:00 的时候关闭，延迟进来的这个59会被忽略;
   * 如果按事件时间来计算，这个窗口当进入一条数据，其EventTime > 10:00的时候会关闭，因为这个数据延迟，会因为别的正常顺序的数据进入Flink导致窗口提前关闭.
   * 即:
   * 处理时间窗口, 按照当前系统时间 来判断进行窗口关闭.
   * 事件时间窗口, 按照进入数据的事件时间 来判断是否关闭窗口，如果进来的一条新数据是下一个窗口的数据，会关闭上一个窗口.
   *
   * WaterMark 水印，用来解决网络延迟问题.
   * WaterMark就是一个时间戳,flink为数据流添加水印。即: 收到一条消息后，额外给其加一个时间字段，就是水印.
   * 一般人为添加的消息的水印比事件时间晚一些，一般几秒钟.
   * 窗口是否关闭，按照水印时间来判断，但原有事件时间不会被修改，窗口的边界依然是事件时间EventTime来决定的.
   * 【当数据流添加水印后，会按水印时间来触发窗口计算】
   * 【接收到的水印时间 >= 窗口的endTime且窗口内有数据，则触发计算】
   *
   * 水印的计算: 事件时间 - 设置的水印长度 = 水印时间.如:
   * EventTime = 10:30, 水印长度2s, 那么水印时间就是 10:28
   *
   * 水印的作用: 在不影响按事件时间判断数据属于哪个窗口的前提下，延迟某个窗口的关闭时间，让其等待一会儿延迟数据.
   * ------- 示例 -------:
   * 窗口5s，水印3s，按事件时间计算:
   * 1、来数据事件时间3，进入窗口0——5，水印时间0;
   * 2、来数据事件时间7，进入窗口5——10，水印时间4;
   * 3、来数据事件时间4，进入窗口0——5，水印时间1;
   * 4、来数据事件时间8，进入窗口5——10，水印时间5;
   * 第4条数据，水印时间 >= 窗口0——5的结束时间，关闭窗口并触发计算.
   *
   * 【多并行度的水印触发】
   * 在多并行度下，每个并行有一个水印,比如并行度=6，即程序中有6个watermark。分别属于这6个并行度（线程）
   * 则: 触发条件以6个水印中最小的那个为准————要等最后一个并行度的水印触发。
   * 注意: 一个窗口可以有多个线程(并行度)在工作的，而不是一个窗口对应一个线程.
   *
   * 一个程序中有多少个水印，和并行度有关，和keyBy无关.
   * 比如list.keyBy(0) 那么分组后的多个组公用一个水印.
   * hadoop满足条件后，会将spark，flink的也触发.
   *
   * ======》 Watermark是一个时间戳，它表示小于该时间戳的事件都已经到达了。
   *
   * 【生成方式】
   * 1、周期性的计算一批新到数据的水印是多少. （更常用）
   * 2、每条数据都进行水印的处理.
   *
   * 例子:
   * 监听9999端口，读取socket数据（格式：name:timestamp）
   * 给当前进入flink的数据加上watermark，值为eventTime - 3s
   * 根据name进行分组，根据窗口大小为5s划分窗口，依次统计窗口中各name的数据.
   */
  def waterMarkTest(senv: StreamExecutionEnvironment): Unit = {
    import org.apache.flink.api.scala._

    senv.setParallelism(1)
    senv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val socketStream = senv.socketTextStream("hiwes", 9999)
    val tuple: DataStream[(String, Long)] = socketStream.map(x => {
      val arr = x.split(" ")
      (arr(0), arr(1).toLong)
    })

    val withWMStream: DataStream[(String, Long)] = tuple.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[(String, Long)] {

      final val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

      // 存储当前最大时间戳
      var currentMaxTimeStamp = 0L

      // 定义最大允许的乱序时间，单位毫秒
      final val maxOutOfOrderness = 0L

      // 定义上一次水印时间
      var oldWaterMarkTS = 0L

      // 用于生成新的水位线，新的水位线只有大于当前水位线才有效。 // todo 指定函数1
      override def getCurrentWatermark: Watermark = {
        val newWaterMarkTS = currentMaxTimeStamp - maxOutOfOrderness
        if (newWaterMarkTS > oldWaterMarkTS) oldWaterMarkTS = newWaterMarkTS
        new Watermark(oldWaterMarkTS)
      }

      // todo 在实际的生产中Periodic的方式必须结合时间和积累条数
      // todo 两个维度继续周期性产生Watermark，否则在极端情况下会有很大的延时。

      // 用于从消息中提取事件时间 // todo 指定函数2
      override def extractTimestamp(element: (String, Long), previousElementTimestamp: Long): Long = {
        val ts = element._2

        if (ts > currentMaxTimeStamp) currentMaxTimeStamp = ts // 将当前的水印时间,赋值给当前时间戳.

        println("word: " + element._1 + ", " +
          "event time: " + simpleDateFormat.format(new Date(ts)) + ", " +
          "current max ts: " + simpleDateFormat.format(new Date(currentMaxTimeStamp)) + ", " +
          "watermark: " + simpleDateFormat.format(new Date(getCurrentWatermark().getTimestamp()))
        )
        ts
      }
    })

    val windowedStream: WindowedStream[(String, Long), Tuple, TimeWindow] = withWMStream.keyBy(0)
      .window(TumblingEventTimeWindows.of(Time.seconds(5)))

    windowedStream.apply(new WindowFunction[(String, Long), (String, Int), Tuple, TimeWindow] {
      final val simpleDataFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

      override def apply(key: Tuple, window: TimeWindow, input: Iterable[(String, Long)], out: Collector[(String, Int)]): Unit = {
        val windowStart = window.getStart
        val windowEnd = window.getEnd
        println("apply-------------------")
        var count = 0
        var word: String = null

        for (element <- input) {
          count += 1
          word = element._1
        }

        out.collect((word, count))
        println("windowStart: " + simpleDataFormat.format(new Date(windowStart)) + ", " +
          "windowEnd: " + simpleDataFormat.format(new Date(windowEnd)))
      }
    }).print()

    senv.execute()
  }

  /**
   * 4.2 水印无法解决长期延迟的情况，比如长期停电之类.
   * 解决这个问题，就需要使用————延迟数据处理机制（allowedLateness方法）
   * waterMark和Window机制解决了流式数据的乱序问题，而allowedLateness机制，
   * 主要的办法就是给定一个允许延迟的时间，在该时间范围内依然可以接受处理延迟数据.
   * 1、设置允许延迟的时间: allowedLateness(lateness:Time)
   * ------ 设置窗口后调用，设置之后，当watermark满足条件，只触发执行，不触发关闭.
   * ------ 如果允许时间内这个窗口还有数据进来，每来一条，就会和已经计算过的数据一起再计算一次.
   * 2、保存延迟数据: sideOutputLateData(outputTag:OutputTag[T])
   * 3、获取延迟数据: DataStream.getSideOutput(tag:OutputTag[X])
   */
  def allowedLatenessTest(senv: StreamExecutionEnvironment): Unit = {
    import org.apache.flink.api.scala._
    senv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    senv.setParallelism(1)

    val socketStream = senv.socketTextStream("hiwes", 9999)

    val tuple = socketStream.map(x => {
      val arr = x.split(" ")
      (arr(0), arr(1).toLong)
    })

    val watermarks = tuple.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[(String, Long)](Time.seconds(3)) {
      override def extractTimestamp(element: (String, Long)): Long = {
        element._2
      }
    })

    val windowedStreamWithLateness = watermarks.keyBy(0).timeWindow(Time.seconds(5))
      .allowedLateness(Time.seconds(2)) // 注意，需要在window之后调用.上面设置水印就是当前时间，这里设置延迟处理时间2s.

    windowedStreamWithLateness.apply(new WindowFunction[(String, Long), (String, Int), Tuple, TimeWindow] {
      override def apply(tuple: Tuple, window: TimeWindow, input: Iterable[(String, Long)], out: Collector[(String, Int)]): Unit = {
        var key: String = null
        var count = 0
        for (ele <- input) {
          count += 1
          key = ele._1
        }
        out.collect((key, count))
        println("window start: " + window.getStart + ", " +
          "window end: " + window.getEnd)
      }
    }).print()

    senv.execute()
  }

  /**
   * 4.3 水印解决乱序问题，延迟数据机制解决长期迟到数据.
   * 但是有的数据来的太久，超过了允许延迟时间怎么处理呢?————侧输出机制Side Output
   * 可以将错过水印又错过allowedLateness允许的时间的数据，单独放在一个DataStream中，
   * 然后开发人员按自定逻辑对这些超级迟到数据进行处理.
   *
   * 处理方式主要2种:
   * 1、对窗口对象调用SideOutputLateData(OutputTag outputTag) 进行数据存储;
   * 2、对DataStream对象调用getSideOutput(OutputTag outputTag) 取出需要单独处理的数据的DataStream。
   * ------ 最后取到的是一个DataStream,所以可以继续写keyBy、window等处理逻辑.
   */
  def sideOutputDataTest(senv: StreamExecutionEnvironment): Unit = {
    import org.apache.flink.api.scala._
    senv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    senv.setParallelism(1)

    val socketStream = senv.socketTextStream("hiwes", 9999)
    val map = socketStream.map(x => {
      val arr = x.split(" ")
      (arr(0), arr(1).toLong)
    })

    val watermarks = map.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[(String, Long)](Time.seconds(3)) {
      override def extractTimestamp(element: (String, Long)): Long = {
        element._2
      }
    })

    val allowedLateness = watermarks.keyBy(0)
      .timeWindow(Time.seconds(5))
      .allowedLateness(Time.seconds(2))

    val outputTag = new OutputTag[(String, Long)]("side output") // 侧输出标签.
    val sideOutputLateData = allowedLateness.sideOutputLateData(outputTag) // 侧输出机制.

    val result = sideOutputLateData.apply(new WindowFunction[(String, Long), (String, Int), Tuple, TimeWindow] {
      override def apply(tuple: Tuple, window: TimeWindow, input: Iterable[(String, Long)], out: Collector[(String, Int)]): Unit = {

        var key: String = null
        var counter = 0
        for (ele <- input) {
          key = ele._1
          counter += 1
        }
        out.collect((key, counter))
      }
    })

    result.print()

    val sideOutput = result.getSideOutput(outputTag)
    sideOutput.print("late >>> ")

    senv.execute()
  }

  /**
   * 5.Flink的异步IO.
   * Async I/O 是阿里巴巴贡献给社区的一个呼声很高的特性，从1.2版本引入。
   * 主要是为了解决与外部系统交互时网络延迟称为系统瓶颈的问题.
   *
   * Flink在做流数据计算的时候，很多时候需要和外部系统进行交互（如:数据库、Redis、Hive、HBase等存储系统），
   * 需要注意系统间通信延迟是否会拖慢整个Flink作业，从而影响整体吞吐量和实时性.
   * 同步模式:
   * --- 查询请求A，等待结果返回，在返回之前，查询请求B、C、D只能等待。
   * --- 极大地阻碍了吞吐和延迟.
   * 异步模式: ===> 目的（提高吞吐量）
   * --- 并发的处理多个请求和回复，可以连续向数据库发送查询请求A、B、C、D，同时哪个请求先回复，
   * --- 就先处理哪个回复，从而连续的请求之前不需要阻塞等待，这就是Async I/O的实现原理.
   * 原因:
   * --- 与数据库的异步交互意味着一个并行函数实例可以同时处理多个请求并同时接收响应（资源复用），
   * --- 这样等待时间可以与发送其他请求和接收响应重叠，至少等待时间是在多个请求上平摊的，这在
   * --- 大多数情况下会导致更高的流吞吐量.
   *
   * 【使用前提】
   * 1、对外部系统进行异步IO访问的客户端API，如Vertx.
   * 2、没有这种客户端的情况下，通过创建多个客户端并使用线程池处理同步调用来尝试同步
   * --- 客户端转变为有限的并发客户端。但是这种方法比适当的异步客户端效率低。
   *
   * 【Async I/O API实现异步流失转换】
   * 允许用户在数据流中使用异步客户端访问外部存储，该API处理与数据流的继承，以及消息顺序性(Order)、
   * 事件时间EventTime、一致性（容错）等脏活累活，用户只专注于业务.
   * 如果目标数据库中有异步客户端，则3步就可实现异步流式转换操作（针对该数据库的异步）:
   * 1、实现用来分发请求的AsyncFunction，用来向数据库发送异步请求并设置回调;
   * 2、获取操作结果的callback，并提交给ResultFuture;
   * 3、将异步I/O操作应用于DataStream.
   *
   * 【Async I/O应用于DataStream】
   * AsyncDataStream是一个工具类，用来将AsyncFunction应用到DataStream,其发出的并发请求都是无序的.
   * 这个顺序基于哪个请求先完成，为了控制结果记录的发出顺序，Flink提供了2种模式，分别对应AsyncDataStream的2个静态方法:
   * 1、OrderedWait(有序).
   * --- 消息的发送顺序和接收到的顺序相同(包括WaterMark)，即: 先进先出.
   * 2、unOrderWait(无序).
   * --- 在ProcessingTime处理时间中，完全无序【哪个请求先返回结果就先发送（最低延迟和最低消耗）】
   * --- 在EventTime事件时间中，以watermark为边界，介于2个watermark之间的消息可以乱序，但watermark和消息之间不能乱序.
   *
   * 以下实例为:
   * 接收socket传入的id值，异步去mysql查询对应结果.
   */
  def asyncIOTest(senv: StreamExecutionEnvironment): Unit = {
    import org.apache.flink.api.scala._

    val socketStream: DataStream[String] = senv.socketTextStream("hiwes", 9999)
    val map: DataStream[Tuple1[Int]] = socketStream.map(new MapFunction[String, Tuple1[Int]] {
      override def map(value: String): Tuple1[Int] = {
        Tuple1(value.toInt)
      }
    })

    // todo 这一步中unorderedWait()有问题，还需要继续验证.
    //    val unorderWait: DataStream[Tuple3[String, Int, String]] = AsyncDataStream.unorderedWait(map,
    //      new MyAsync(), 60000L, TimeUnit.MILLISECONDS)

    socketStream.print("socket >>> ")
    //    unorderWait.print()

    senv.execute()
  }

  // todo 需要重新关注Async I/O的使用.
  //  class MyAsync extends RichAsyncFunction[Tuple1[Int], Tuple3[String, Int, String]] {
  //    private var connection: Connection = null
  //    private var ps: PreparedStatement = null
  //
  //    override def open(parameters: Configuration): Unit = {
  //      super.open(parameters)
  //      super.open(parameters)
  //      Class.forName("com.mysql.jdbc.Driver")
  //
  //      val url = "jdbc:mysql://hiwes:3306/test?useUnicode=true&characterEncoding=utf-8&useSSL=false"
  //      this.connection = DriverManager.getConnection(url, "root", "root")
  //
  //      this.ps = connection.prepareStatement("select username,password,name  from user where id = ?")
  //    }
  //
  //    override def close(): Unit = {
  //      super.close()
  //
  //      if (this.ps != null) this.ps.close()
  //      if (this.connection != null) this.connection.close()
  //    }
  //
  //    override def asyncInvoke(input: Tuple1[Int], resultFuture: ResultFuture[(String, Int, String)]): Unit = {
  //      CompletableFuture.supplyAsync(new Supplier[Tuple3[String, Int, String]] {
  //        override def get(): (String, Int, String) = {
  //
  //          println("将要查询: " + input._1)
  //          var username: String = null
  //          var password = 0
  //          var name: String = null
  //          try {
  //            ps.setInt(1, input._1)
  //            val resultSet = ps.executeQuery()
  //            if (resultSet.next()) {
  //              username = resultSet.getString("username")
  //              password = resultSet.getInt("password")
  //              name = resultSet.getString("name")
  //              Thread.sleep(10000L)
  //            }
  //          } catch {
  //            case e: SQLException => e.printStackTrace()
  //            case e: InterruptedException => e.printStackTrace()
  //          }
  //          Tuple3(username, password, name)
  //        }
  //      }).thenAccept(new Consumer[Tuple3[String, Int, String]] {
  //        override def accept(t: Tuple3[String, Int, String]): Unit = {
  //          val list = List[(String, Int, String)](t._1, t._2, t._3)
  //          resultFuture.complete(list)
  //        }
  //      })
  //
  //
  //    }
  //
  //    def convertIterableFromIterator(iterator2: Iterator[(String, Int, String)]): Iterable[(String, Int, String)] = new Iterable[(String, Int, String)]() {
  //      override def iterator(): Iterator[(String, Int, String)] = iterator2
  //    }
  //  }

  /**
   * 6.Flink的状态State管理.
   * 注意:
   * 1] state一般指一个具体的task/operator的状态。
   * 2] 而checkpoint则表示了一个Flink Job，在一个特定时刻的一份全局状态快照，即包含了所有task/operator的状态。
   *
   * 无状态流处理，每次只能转换一条输入记录，且仅根据最新的输入记录输出结果.
   * 需要使用状态的场景举例:
   * 1、去重.  记录所有的主键。
   * 2、窗口计算.  已进入的未触发的数据。
   * 3、机器学习/深度学习.  训练的模型及参数。
   * 4、访问历史数据 需要与昨日进行对比。
   *
   * 状态，是基于Checkpoint检查点机制来完成状态的持久化。
   * 在未进行Checkpoint之前，State是在内存中的（变量），当checkpoint后，State被序列化持久化保存.
   * Checkpoint支持多种持久化存储，比较常用的有File、HDFS和S3等.
   *
   * 【State的类型划分】   Flink有2种基本类型的State:
   * 1、Keyed State 键控状态.
   * 就是基于KeyedStream上的状态。这个状态是跟特定的key绑定的，对KeyedStream流上的每一个key，可能都对应一个state。
   * 2、Operator State 算子状态.
   * 跟一个特定operator的一个并发实例绑定，整个operator只对应一个state。
   *
   * 相比较而言，在一个operator上，可能会有很多个key，从而对应多个keyed state。
   *
   * Flink中的Kafka Connector，就使用了operator state。它会在每个connector实例中，保存该实例中消费topic的所有(partition, offset)映射。
   *
   * 而这两种类型，可以以2种形式存在:
   * 1、raw state 原始状态.
   * --- 原生态State。需要用户自己管理和序列化，数据结构为：字节数组。自定义Operator时使用。
   *
   * 2、managed state 托管状态.
   * --- flink自动管理的State。Runtime管理、自动存储自动回复，内存管理上可自动优化。数据结构为：ValueState, ListState, MapState等。大多数情况均可用.
   */
  def stateCheckpointTest(senv: StreamExecutionEnvironment): Unit = {
    // Keyed State。键控状态
    keyedStateCheckpoint(senv)

    // Operator State。算子状态
    operatorStateCheckpoint(senv)

    // Broadcast State。广播变量状态
    broadcastStateCheckpoint(senv)

    senv.execute()
  }

  /**
   * 6.1 Keyed State。键控状态
   *
   * 基于keyedStream上的状态，跟特定key绑定。对于keyedStream流上每个key都对应一个State.
   * 这些状态仅可以在KeyedStream上使用，可通过stream.keyBy(...)得到keyedStream.
   * 保存State的数据结构有:
   * 1、ValueState[T] 类型为T的单值状态.
   * --- get操作: ValueState.value()
   * --- set操作: ValueState.update(value:T)
   * 2、ListState[T] 即key上的状态值为一个列表，列表元素数据类型为T.
   * --- 附加值操作: ListState.add(value:T)
   * --- 附加值操作: ListState.addAll(values: List[T])
   * --- 获取值操作: ListState.get()  返回Iterable[T]
   * --- set操作: ListState.update(values: List[T])
   * 3、MapState[UK, UV] 即状态值为一个map，用户通过put和putAll添加数据.
   * --- MapState.get(key: K)
   * --- MapState.put(key: K, value: V)
   * --- MapState.contains(key: K)
   * --- MapState.remove(key: K)
   * 4、ReducingState[T] 这种状态通过用户传入的ReduceFunction.
   * 每次调用add()添加值的时候，会调用reduceFunction()，最后合并到一个单一的状态值.
   * --- ReducingState.add(value: T)
   * --- ReducingState.addAll(values: List[T])
   * --- ReducingState.get()
   * --- ReducingState.undate(values: List[T])
   * 5、FoldingState[T] 跟ReducingState有点类似。【在当前版本已经删除】 @deprecated
   * --- 不过它的状态值类型可以与add方法中传入的元素类型不同。
   * 6、所有类型的状态还有一个clear()方法，清除当前key下的状态数据，也就是当前输入元素的Key.
   *
   * 【注意】
   * 以上所有的State对象，仅用于与状态进行交互（更新、删除、清空等），
   * 真正的状态值，可能存在内存、磁盘或其他分布式存储系统汇总。我们只是持有这个状态的句柄.
   * 必须撞见一个StateDescriptor才能得到对应的状态句柄。根据不同的状态类型，可创建:
   * 1、ValueStateDescriptor.
   * 2、ListStateDescriptor.
   * 3、ReducingStateDescriptor.
   * 4、MapStateDescriptor.
   *
   * 状态通过RuntimeContext进行访问，因此只能在Rich Function中使用。提供了以下方法:
   * 1、def getState(vsd:ValueStateDescriptor[T]):ValueState = {}
   * 2、def getReducingState(rsd:ReducingStateDescriptor[T]):ReducingState = {}
   * 3、def getListState(lsd:ListStateDescriptor[T]):ListState = {}
   * 4、def getAggregatingState(asd:AggregatingStateDescriptor[IN, ACC, OUT]): AggregatingState[IN, OUT] = {}
   * 5、def getMapState(msd:MapStateDescriptor[UK,UV]):MapState[UK, UV] = {}
   *
   * @param senv
   */
  def keyedStateCheckpoint(senv: StreamExecutionEnvironment): Unit = {

    import org.apache.flink.api.scala._

    senv.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE)

    /**
     * Flink 自带了以下几种开箱即用的 state backend：
     * 1.MemoryStateBackend 默认.小状态，本地调试使用
     * 2.FsStateBackend 大状态，长窗口，高可用场景
     * 3.RocksDBStateBackend 超大状态，长窗口，高可用场景，可增量checkpoint
     * 但是现在的StateBackend是直接使用StateBackend类
     * todo 这里还需要后续进行补充.
     */
    senv.setStateBackend(new FsStateBackend("file:///Users/hiwes/data/output/checkpoint"))
    //    senv.setStateBackend(new StateBackend {
    //      override def resolveCheckpoint(externalPointer: String): CompletedCheckpointStorageLocation = ???
    //
    //      override def createCheckpointStorage(jobId: JobID): CheckpointStorage = ???
    //
    //      override def createKeyedStateBackend[K](env: Environment, jobID: JobID, operatorIdentifier: String, keySerializer: TypeSerializer[K], numberOfKeyGroups: Int, keyGroupRange: KeyGroupRange, kvStateRegistry: TaskKvStateRegistry, ttlTimeProvider: TtlTimeProvider, metricGroup: MetricGroup, stateHandles: util.Collection[KeyedStateHandle], cancelStreamRegistry: CloseableRegistry): AbstractKeyedStateBackend[K] = ???
    //
    //      override def createOperatorStateBackend(env: Environment, operatorIdentifier: String, stateHandles: util.Collection[OperatorStateHandle], cancelStreamRegistry: CloseableRegistry): OperatorStateBackend = ???
    //    })

    val socketStream = senv.socketTextStream("hiwes", 9999)
    val map = socketStream.map(x => {
      val arr = x.split(" ")
      (arr(0), arr(1).toInt)
    })

    val keyedStream = map.keyBy(0)

    val reduceState = keyedStream.reduce(new RichReduceFunction[(String, Int)] {
      private var state: ValueState[(String, Int)] = null

      override def open(parameters: Configuration): Unit = {
        super.open(parameters)

        /**
         * 调用了RichFlatMapFunction.getRuntimeContext().getState方法，
         * 最终会调用StreamingRuntimeContext.getState方法。
         * public <T> ValueState<T> getState(ValueStateDescriptor<T> stateProperties) {
         * KeyedStateStore keyedStateStore = checkPreconditionsAndGetKeyedStateStore(stateProperties);
         * stateProperties.initializeSerializerUnlessSet(getExecutionConfig());
         * return keyedStateStore.getState(stateProperties);
         * }
         * checkPreconditionsAndGetKeyedStateStore方法中：
         * KeyedStateStore keyedStateStore = operator.getKeyedStateStore();
         * return keyedStateStore;
         */
        state = getRuntimeContext.getState(new ValueStateDescriptor[(String, Int)](
          "reduceState",
          TypeInformation.of(new TypeHint[(String, Int)] {
          })
        ))
      }

      override def reduce(value1: (String, Int), value2: (String, Int)): (String, Int) = {
        val result = (value1._1, value1._2 + value2._2)
        state.update(result)
        result
      }
    })

    reduceState.print()
  }

  // 自定义数据源，实现不断产生数据并进行checkpoint，保存数据的偏移量到ListState中.
  def operatorStateCheckpoint(senv: StreamExecutionEnvironment): Unit = {
    import org.apache.flink.api.scala._

    senv.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE)

    senv.setStateBackend(new FsStateBackend("file:///Users/hiwes/data/output/checkpoint"))

    val source = senv.addSource(new MySourceWithState())

    source.print()
  }

  class MySourceWithState extends SourceFunction[Int] with CheckpointedFunction {

    var isRun = true
    var state: ListState[Int] = null
    var currentCounter = 0

    override def snapshotState(context: FunctionSnapshotContext): Unit = {
      // 清除旧数据.
      this.state.clear()

      // 添加新数据.
      this.state.add(this.currentCounter)
    }

    override def initializeState(context: FunctionInitializationContext): Unit = {
      this.state = context.getOperatorStateStore.getListState(new ListStateDescriptor[Int](
        "operatorState",
        TypeInformation.of(new TypeHint[Int]() {})
      ))

      val ite = this.state.get().iterator()
      while (ite.hasNext) {
        val counter = ite.next()
        this.currentCounter = counter
      }
      this.state.clear()
    }

    override def run(ctx: SourceFunction.SourceContext[Int]): Unit = {
      while (this.isRun) {
        this.currentCounter += 1
        ctx.collect(this.currentCounter)
        TimeUnit.SECONDS.sleep(1L)
        if (this.currentCounter == 10)
          println("手动抛出异常: " + (1 / 0))
      }
    }

    override def cancel(): Unit = {
      this.isRun = false
    }
  }

  /** 从1.5版本引入，主要解决
   * 【一条流需要根据规则或配置处理数据，但是规则或配置优势随时变化的】
   * 将规则或配置作为广播流广播出去，并以Broadcast State的形式存储在下游Task中，根据其中的规则或配置来处理常规流中的数据.
   * 【注意】
   * 1、Broadcast State是Map类型，即K-V类型;
   * 2、Broadcast State只有在广播的一侧可以修改，在非广播一次是只读;
   * 3、Broadcast State中元素的顺序，在各Task中可能不同，基于顺序的处理时需要注意;
   * 4、Broadcast State在Checkpoint时，每个Task都会Checkpoint广播状态;
   * 5、Broadcast State在运行时保存在内存中，目前还不能保存在RocksDB State Backend中.
   * 【实例】
   * Source1来自socket套接字 输入id数字
   * Source2来自自定义Source，每隔5s生成对应的广告数据
   * 将广告数据Source装变为BroadcastDataStream对象
   * 基于socket的Source connect 广告Source，得到BroadcastConnectedStream
   * 对BroadcastConnectedStream调用process()分别处理数据流和广播流.
   * 打印结果，查看动态变化。
   */
  def broadcastStateCheckpoint(senv: StreamExecutionEnvironment): Unit = {
    import org.apache.flink.api.scala._
    senv.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE)
    senv.setStateBackend(new FsStateBackend("file:///Users/hiwes/data/output/checkpoint"))

    val adSource = senv.addSource(new MySourceForBroadcast())
    val idDataStream = senv.socketTextStream("hiwes", 9999).map(_.toInt)

    val broadcastState = adSource.broadcast(new MapStateDescriptor[Int, (String, String)](
      "broadcast state",
      TypeInformation.of(new TypeHint[Int] {}),
      TypeInformation.of(new TypeHint[(String, String)] {})
    ))

    val connectedStream = idDataStream.connect(broadcastState)

    val process = connectedStream.process(new BroadcastProcessFunction[Int, Map[Int, (String, String)], (String, String)] {
      final val mapStateDescriptor = new MapStateDescriptor[Int, (String, String)](
        "broadcast state",
        TypeInformation.of(new TypeHint[Int] {}),
        TypeInformation.of(new TypeHint[(String, String)] {})
      )

      override def processElement(value: Int, ctx: BroadcastProcessFunction[Int, Map[Int, (String, String)], (String, String)]#ReadOnlyContext, out: Collector[(String, String)]): Unit = {
        val state = ctx.getBroadcastState(mapStateDescriptor)
        val tuple2 = state.get(value)
        if (tuple2 != null) out.collect(tuple2)
      }

      override def processBroadcastElement(value: Map[Int, (String, String)], ctx: BroadcastProcessFunction[Int, Map[Int, (String, String)], (String, String)]#Context, out: Collector[(String, String)]): Unit = {
        val state = ctx.getBroadcastState(mapStateDescriptor)
        state.clear()
        val mapp = new util.HashMap[Int, (String, String)]() // 增加scala map转 java map过程.
        for (v <- value) {
          mapp.put(v._1, v._2)
        }
        state.putAll(mapp)
      }
    })

    /**
     * BroadcastState 代码执行流程解析.
     * SocketSource ---> 流式传输输入值 ---> DataStream
     * =========================================> 二者connect: BroadcastConnectedStream
     * MySQLSource ---> 每隔一段时间查询一次 ---> DataStream ---> 调用broadcast ---> BroadcastStream
     *
     * BroadcastConnectedStream ---> process方法处理 ---> 自定义BroadcastProcessFunction
     * ---> processElement()，处理数据流，通过关联广播流得到返回输出 ---> 返回数据转换为普通DataStream ---> DataStream
     * ---> processBroadcastProcess()，更新BroadcastState的值(MySQL中查询的新数据)
     */
    process.print()

  }

  class MySourceForBroadcast extends SourceFunction[Map[Int, (String, String)]] {
    private final val random = new Random()
    private final val ads: List[(String, String)] = List(
      ("baidu", "搜索引擎"),
      ("google", "科技大牛"),
      ("aws", "高级云平台"),
      ("aliyun", "高级云平台"),
      ("tecent", "氪金达人"),
      ("alibaba", "电商龙头"),
      ("zijie", "算法大佬"),
      ("meituan", "黄色公司"),
      ("eleme", "蓝色公司"),
      ("ruixin", "还挺好喝")
    )

    private var isRun = true

    override def run(ctx: SourceFunction.SourceContext[Map[Int, (String, String)]]): Unit = {
      while (this.isRun) {
        val map = Map[Int, (String, String)]()

        var keyCounter: Int = 0
        for (i <- 0 until ads.size) {
          keyCounter += 1
          map.+(keyCounter -> ads(random.nextInt(ads.size)))
        }
        ctx.collect(map)
        TimeUnit.SECONDS.sleep(5L)
      }
    }

    override def cancel(): Unit = {
      this.isRun = false
    }
  }

  /**
   * 7.Flink的容错机制.
   * 为了让Flink的State具有良好的容错性，Flink提供了checkpoints检查点机制。Flink定期在
   * 数据流上生成checkpoint barrier，当某个算子收到barrier时，即会基于当前状态生成一份快照，
   * 然后再将该barrier传递到下游算子，下游算子接收到该barrier后，也基于当前状态生成一份快照，
   * 依次传递直到最后的Sink算子上，当出现异常后，Flikn可按照最近一次快照数据将所有算子恢复到先前状态.
   *
   * 1、CheckpointCoordinator周期性向该流应用的所有Source算子发送barrier;
   * 2、当某个Source算子收到一个barrier时，暂停数据处理过程，然后将自己的当前状态制作成快照，并保存到
   * 指定的持久化存储中，然后向CheckpointCoordinator报告自己制作情况，同时向自身所有下游算子广播该barrier，自己恢复数据处理
   * 3、下游算子收到barrier时，会暂停自己的数据处理过程，将自己的当前状态制作成快照，保存到指定的
   * 持久化存储中，然后向CheckpointCoordinator报告自身快照情况，同时向所有下游算子广播该barrier，自己恢复数据处理.
   * 4、每个算子按照步骤3制作快照并向下游广播，直到最后barrier传递到Sink算子，快照制作完成;
   * 5、当CheckpointCoordinator收到所有算子报告之后，认为该周期的快照制作完毕，否则：
   * 如果在规定时间内没有收到所有算子的报告，认为本周期快照制作失败.
   *
   * 【单流的Barrier】
   * Flink分布式快照的核心概念之一就是Barrier（数据栅栏）.
   * 1、这些barrier被插入到数据流中，作为流的一部分和数据一起向下流动，可以理解为特殊的插队数据，前面的数据没有处理掉，barrier也不会前进.
   * --- 当barrier跟随数据流走到具体的Operator时，就会告知Operator进行快照;
   * --- Barrier走到哪里，就阻塞哪里的数据，等当前Operator完成快照后，barrier告知CheckpointCoordinator某个operator完成快照，然后继续流动;
   * --- 如此反复，直到Sink算子，Barrier会阻塞它后面的数据流（但不会很慢）
   * 2、Barrier非常轻量，不会干扰正常数据，数据流严格有序，Barrier永远不会赶超通常的流记录，会严格遵循顺序;
   * 3、一个Barrier把数据流分割为2部分:一部分进入当前快照，一部分进入下一个快照;
   * 4、每个Barrier带有一个快照ID，且barrier之前的数据都进入此快照;
   * 5、当Barrier走完所有算子后，通知CheckpointCoordinator，整体任务完成。
   * 6、多个不同快照的多个barrier会在流中同时出现，【即:多个快照可能同时创建】
   * --- 由于一些原因(比如Flink执行过慢、或者Checkpoint设置的过于频繁)，导致上一个barrier还未完成任务（90%）,
   * --- 新发送的barrier已经跟随着数据流排队走到第一个operator了，这种情况也是允许的，两个barrier各自干活互不影响.
   *
   * 【2个输入源的Checkpoint】
   * 当一个算子有2个输入源，则暂时阻塞先收到barrier的输入源，等第二个输入源相同编号的
   * barrier到来时，再制作自身快照并向下游广播。
   *
   * 【持久化存储】
   * Checkpoint的持久化存储可以用如下3种:
   * 1、MemoryStateBackend 默认.小状态，本地调试使用
   * 2、FsStateBackend 大状态，长窗口，高可用场景
   * 3、RocksDBStateBackend 超大状态，长窗口，高可用场景，可增量checkpoint
   *
   * 【MemoryStateBackend】
   * 将状态维护在Heap堆上的一个内部状态后端，键值状态和窗口算子使用哈希表来存储数据（Values）和定时器（Timers）。
   * 注意点: 【在debug模式使用，不建议在生成模式下应用】
   * 1、默认情况，每个State大小限定5MB，可通过MemoryStateBackend的构造函数增加此大小;
   * 2、State受到Akka帧大小 限制，无论怎么调整，都不能大于Akka帧大小;
   * 3、State总大小不能超过JobManager的内存;
   * 什么时候使用:
   * 1、本地开发或调试时建议使用MemoryStateBackend，因为此时状态大小有限;
   * 2、最适合小State的应用场景，如:Kafka Consumer，或一次仅一记录的函数.
   *
   * 【FsStateBackend】
   * 主要是将快照数据保存在文件系统中，主要是HDFS和File. 分别对应hdfs://和file:///路径.
   * 在分布式情况下，不推荐使用本地文件，如果某个算子在节点A失败，在节点B恢复，使用本地文件时，会无法读取A上的数据导致State恢复失败;
   * 注意点:【分布式文件持久化，每次读写都产生网路oIO，整体性能不佳】
   * 1、具有大State、长Window、大 键/值 状态的作业;
   * 2、所有高可用性设置.
   *
   * 【RocksDBStateBackend】
   * 配置也需要一个文件系统，一般来说
   * hdfs://hiwes:8020/flink/checkpoints
   * file:///Users/hiwes/data/flink/checkpoints
   * RocksDB是一种嵌入式的本地数据库，在处理时将RocksDB存储到本地磁盘上，在Checkpoint的时候，
   * 整个RocksDB数据库会被存储到配置的文件系统中，或者在超大State作业时可以将增量的数据存储到配置的文件系统中.
   * 同时,Flink会将极少的元数据存储在JobManager的内存中，或者Zookeeper中(对于HA情况).
   * RocksDB默认也是配置为异步快照的模式.
   * 注意点:【本地文件 + 异步HDFS持久化】
   * 1、最适合用来处理大State、长Windiw、大 键/值 状态的有状态处理任务;
   * 2、非常适合用于HA方案;
   * 3、是目前唯一支持增量checkpoint的后端。增量checkpoint很适用于超大State的场景.
   */
  def checkpointTest(senv: StreamExecutionEnvironment): Unit = {
    import org.apache.flink.api.scala._

    // 设置checkpoint周期执行时间.
    senv.enableCheckpointing(5000)
    // 设置checkpoint执行模式，最多执行一次或最少执行一次.
    senv.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    // 设置checkpoint的超时时间
    senv.getCheckpointConfig.setCheckpointTimeout(60000)
    // 如果在制作快照过程中出现错误，是否让整体任务失败.
    senv.getCheckpointConfig.setFailOnCheckpointingErrors(false)
    // 设置同一时间有多少个checkpoint可以同时执行
    senv.getCheckpointConfig.setMaxConcurrentCheckpoints(1)

    /**
     * 修改StateBackend的2种方式.
     */
    // 单任务调整StateBackend.
    senv.setStateBackend(new FsStateBackend("hdfs://hiwes:8020/flink/checkpoints"))

    // 全局调整.
    // 修改flink-conf.yaml:
    // state.backend:filesystem || jobmanager || rocksdb
    // state.checkpoints.dir: hdfs://hiwes:8029/flink/checkpoints
    //
    // 其中state.backend的值可以使以下几种:
    // jobmanager(MemoryStateBackend)  filesystem(FsStateBackend)  rocksdb(RocksDBStateBackend)

    /**
     * 默认情况下，checkpoint不是持久化的，只用于从故障中恢复作业。
     * 当程序被取消时会被删除。但可以配置checkpoint被周期性持久化到外部，类似于savepoints.
     * 当工作失败了，就会有一个checkpoint来恢复。
     */
    senv.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    // ExternalizedCheckpointCleanup模式配置当你取消作业时外部checkpoint会产生什么行为.
    //    RETAIN_ON_CANCELLATION : 保留外部checkpoint，需要手动清理
    //    DELETE_ON_CANCELLATION : 删除外部化的checkpoint，当作业失败时，检查点状态才可用

    senv.execute()
  }

  /**
   * 7.2 Checkpoint的高级选项.
   */
  def checkpointTest2(senv: StreamExecutionEnvironment): Unit = {
    import org.apache.flink.api.scala._

    senv.enableCheckpointing(5000)
    //    senv.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    senv.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE)
    // 默认检查点不被保留，所以可启用外部持久化检查点,并指定保留策略.
    senv.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    // Checkpointing的超时时间，超时未完成，则被终止.
    senv.getCheckpointConfig.setCheckpointTimeout(60000)
    // CheckPointing的最小时间间隔，用于指定上一个checkpoint完成之后最小等多久可以触发另一个checkpoint
    senv.getCheckpointConfig.setMinPauseBetweenCheckpoints(500)
    // 指定运行中的Checkpoint最多可以有多少个
    senv.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    // 用于指定Checkpoint发生异常时，是否该fall这个task，默认为true。设定为false，则task会拒绝Checkpoint然后继续运行.
    senv.getCheckpointConfig.setFailOnCheckpointingErrors(false)

    senv.execute()
  }

  /**
   * 7.3 Flink的重启策略.
   * 当Task发生故障，Flink需重启出错的Task以及其他受到影响的Task，以使得作业恢复到正常执行状态.
   * Flink通过重启策略和故障恢复策略来控制Task重启:
   * 重启策略   决定是否可以重启以及重启的间隔;
   * 故障恢复策略 决定哪些Task需要重启.
   * 如果未提交重启策略，则使用flink-conf.yaml指定的默认策略，否则会用提交的策略覆盖默认策略.
   * 在flink-conf.yaml中，restart-strategy定义了哪种策略会被采用:
   * 如果Checkpoint未启动，就采用no restart策略;
   * 如果Checkpoint启动，但未指定重启策略，则采用fixed-delay策略，重试 Integer.Max_VALUE 次;
   * fixed-delay  固定延迟重启策略.
   * failure-rate 失败率重启策略
   * none         无重启策略.
   *
   * 当然也可以为每一个Job指定自己的重启策略，可以在: env.setRestartStrategy()来程序化地调用。
   *
   * 【fixed-delay策略】
   * 会尝试一个给定的次数来重启Job，超过最大次数则Job最终失败。在连续两次重启尝试之间，会等待一个固定的时间;
   * 配置参数:
   * > restart-strategy: fixed-delay
   * > restart-strategy.fixed-delay.attempts: 3   # 尝试次数
   * > restart-strategy.fixed-delay.delay: 10 s   # 间隔时间
   *
   * 【failure-rate策略】
   * 在Job失败后重启，但是超过失败率后会认定Job失败.在连续两次重启尝试之间，会等待一个固定的时间;
   * 配置参数:
   * > restart-strategy: failure-rate
   * > restart-strategy.failure-rate.max-failures-per-interval: 3
   * > restart-strategy.failure-rate.failure-rate-interval: 5 min
   * > restart-strategy-failure-rate.delay: 10s
   *
   * 【none策略】
   * Job直接失败，不尝试进行重启.
   * 配置参数:
   * > restart-strategy: none
   */
  def checkpointTest3(senv: StreamExecutionEnvironment): Unit = {
    import org.apache.flink.api.scala._

    // 使用Fixed-dalay策略. === 检测3次，每次检测间隔1s
    senv.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 1000l))

    // 使用failure-rate策略.  === 每5分钟内失败3次，每次检测间隔10s，则job失败
    senv.setRestartStrategy(RestartStrategies.failureRateRestart(3, // 每个时间间隔最大失败次数
      org.apache.flink.api.common.time.Time.of(5, TimeUnit.MINUTES), // 失败率测量的时间间隔
      org.apache.flink.api.common.time.Time.of(10, TimeUnit.SECONDS))) // 两次连续重启尝试的时间间隔

    // 使用none策略. === 不进行job重启.
    senv.setRestartStrategy(RestartStrategies.noRestart())

    senv.execute()
  }

  /**
   * 实例: 基于单词统计案例，当遇到"laowang"的时候，抛出异常，出现3次后程序退出.
   *
   * @param senv
   */
  def checkpointTestLast(senv: StreamExecutionEnvironment): Unit = {
    import org.apache.flink.api.scala._

    senv.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE)

    senv.setStateBackend(new FsStateBackend("file:///Users/hiwes/data/output/checkpoints"))

    // 固定延迟重启策略: 当程序出现异常时重启三次，每次5s间隔，超过3次程序退出.
    senv.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,
      org.apache.flink.api.common.time.Time.of(5, TimeUnit.SECONDS)
    ))

    // 失败率策略: 当程序在一分钟内失败3次，则程序退出，每次5s间隔
//    senv.setRestartStrategy(RestartStrategies.failureRateRestart(3,
//      org.apache.flink.api.common.time.Time.minutes(1),
//      org.apache.flink.api.common.time.Time.seconds(5))
//    )

    val socketStream = senv.socketTextStream("hiwes", 9999)

    val wordAndOne = socketStream.map(x => {
      if (x.startsWith("laowang"))
        throw new RuntimeException("老王来了，程序挂了!!!")
      (x, 1)
    })

    val result = wordAndOne.keyBy(0).sum(1)

    result.print()

    senv.execute()
  }

  /**
   * 7.4 SavePoint.
   * SavePoint 是根据Flink Checkpointing机制所创建的流作业执行状态的一致镜像.
   * 可以使用SavePoint进行Flink作业的停止与重启、fork或更新.
   * SavePoint由2部分组成:
   * 1、稳定存储（HDFS、S3）上包含二进制文件的目录（一般很大）
   * --- 稳定存储上的文件表示作业执行状态的数据镜像.
   * 2、元数据文件（相对较小）
   * --- 以绝对路径的形式包含(主要)指向作为SavePoint一部分的稳定存储上的所有文件的指针.
   *
   * 从概念上讲，SavePoint和CheckPoint的区别，类似于RDBMS中的备份与恢复日志的差异.
   * Checkpoint主要目的是为意外失败的作业提供恢复机制.
   * Checkpoint的生命和走起由Flink管理，无需用户交互.
   * Checkpoint的目标是: 轻量级创建 | 尽可能快地恢复.
   *
   * 相反，SavePoint由用户创建、拥有和删除，他们的用例是计划的，手动备份和回复.
   * 比如:
   * 升级Flink版本，调整用户逻辑，改变并行度，以及进行蓝绿部署等.
   * SavePoint必须在作业停止后继续存在.
   *
   * 从概念上讲，SavePoint的生成，恢复成本更高。
   * 它更多的关注可移植性和对作业更改的支持.
   *
   * Checkpoint和SavePoint的实现基本使用相同的代码.
   *
   * 【集群提交】
   * # 运行Job
   * > bin/flink run --class com.hiwes.flink.Zinterview.highlevel_4.FlinkHighLevel1
   * /Users/hiwes/ownprojects/FlinkTest/target/FlinkTest-1.0.jar
   *
   * # 创建Savepoint
   * > bin/flink savepoint 91baf1266f88370600ef33e86c1fb3bf hdfs://hiwes:8020/output/savepoint
   *
   * # 停止Job
   * > bin/flink cancel 91baf1266f88370600ef33e86c1fb3bf
   *
   * # 重新启动Job，加载savepoint数据.
   * > bin/flink run -s hdfs://hiwes:8020/output/savepoint/savepoint-91baf1-84ff05ea4195
   * --class com.hiwes.flink.Zinterview.highlevel_4.FlinkHighLevel1
   * /Users/hiwes/ownprojects/FlinkTest/target/FlinkTest-1.0.jar
   *
   */
  def savePointTest(senv: StreamExecutionEnvironment): Unit = {
    import org.apache.flink.api.scala._

    senv.setParallelism(1)

    senv.enableCheckpointing(5000l, CheckpointingMode.EXACTLY_ONCE)
    senv.setStateBackend(new FsStateBackend("file:///Users/hiwes/data/output/checkpoint"))
    senv.setRestartStrategy(RestartStrategies.fixedDelayRestart(5,
      org.apache.flink.api.common.time.Time.seconds(5)))

    senv.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    senv.getCheckpointConfig.setMinPauseBetweenCheckpoints(500l)
    senv.getCheckpointConfig.setCheckpointTimeout(10000l)
    senv.getCheckpointConfig.setMaxConcurrentCheckpoints(1)

    val source = senv.addSource(new MySourceForGenerateNumberAccumulate())
    source.print()

    senv.execute()

  }

  class MySourceForGenerateNumberAccumulate extends SourceFunction[Int] with CheckpointedFunction {

    private var isRun = true
    private var counter = 0
    private var state: ListState[Int] = null

    override def snapshotState(context: FunctionSnapshotContext): Unit = {
      this.state.clear()
      this.state.add(counter)
    }

    override def initializeState(context: FunctionInitializationContext): Unit = {
      this.state = context.getOperatorStateStore.getListState(new ListStateDescriptor[Int]("counter", TypeInformation.of(new TypeHint[Int] {})))

      val ite = this.state.get().iterator()
      while (ite.hasNext) {
        val i = ite.next()
        this.counter = i
      }
    }

    override def run(ctx: SourceFunction.SourceContext[Int]): Unit = {
      while (this.isRun) {
        counter += 1
        ctx.collect(counter)
        TimeUnit.SECONDS.sleep(1l)
      }
    }

    override def cancel(): Unit = {
      this.isRun = false
    }
  }

  /**
   * 8.Flink的End-to-End Exactly-Once语义.
   *
   * 【什么是状态一致性】
   * Flink相对其他流计算引擎，最突出的特点就是: 状态的管理
   * 对于流处理器内部来说，所谓的状态一致性，就是【计算结果要保证准确】
   * --- 一条数据不应该丢失，也不应该重复计算;
   * --- 再遇到故障时可以恢复状态State,恢复之后的重新计算，结果也是完全正确的.
   *
   * 状态一致性的分类:
   * 1、AT-MOST-ONCE 最多一次.
   * 最多处理一次事件，任务故障的时候，什么都不做。既不恢复丢失的State，也不重播丢失的数据.
   * 2、AT-LEAST-ONCE 至少一次.
   * 不希望丢失事件.有些事件可能被处理多次.
   * 3、EXACTLLY-ONCE 恰好一次.
   * 没有事件丢失，针对每一个数据，内部状态仅更新一次.
   *
   * 【流计算中的一致性语义】
   * 对于批处理分析，容错性很好做，失败的时候只需要replay即可做到完美容错.
   * 但是对于流处理分析来说，数据流本身是动态地，没有所谓的开始或结果，虽然可以replay buffer的部分数据，但是容错做起来更加复杂.
   * 分布式情况下，由多个Source节点、多个Operator(数据处理)节点、多个Sink节点组成，其中:
   * --- 每个节点的并行数可以有差异，且每个节点都可能发生故障;
   * 重点就在于________发生故障时，怎样容错和恢复.
   * 1、最简单的恢复方式: At Most Once
   * 发生故障后重启，直接处理下个数据，不理会之前错误的数据
   * 适用于 要求不高的业务场景.
   *
   * 2、对计算能正确性要求变高: At Least Once
   * 发生故障后重启进行重试，不关心有没有重复数据，需要人工干预自己处理重复数据.
   *
   * 3、要求更准确的计算: Eactly Once
   * 在计算过程汇总，将计算节点状态存储，当计算节点发生故障，可以从存储的State中恢复;
   * 正确计算一次，只能保证内部体系的正确一次计算，无法保证外部体系也是正确一次.
   * 比如: Flink计算节点故障，期间已经有一部分数据Sink出去了。此时从计算节点存储状态中恢复后会产生重复数据.
   *
   * 4、端到端的一致性: End-To-End Exactly Once
   * 实现端到端的一致性语义，需要通过Flink-Kafka实现.
   * 保证全部记录仅影响内部和外部状态一次.
   * ----------------------------------------------------------------
   * 【流计算系统如何支持一致性语义】
   * 1、At Least Once + 去重
   * --- 每个算子维护一个事务日志，根据已经处理的事件，重放失败的事件，在事件进入下个算子之前，移除重复的事件;
   * --- 优点: 故障对性能的影响是局部的.
   * --- 缺点: 存储系统会存储大量的数据，每个算子的每个事件都有性能开销
   *
   * 2、At Least Once + 幂等。  依赖Sink端存储的去重性和数据特征.
   * 分布式快照算法（异步屏蔽快照）:
   * --- 引入Barrier，把输入的数据流分为两部分: preshot records 和 postshot records
   * --- Operator收到所有上游Barrier的时间做一个SnapShot，存储到内部的State Backend里;
   * --- 当全部Sink Operator都完成snapshot，这一路的snapshot就完成了.
   * --- 优点: 较小的性能和资源开销;
   * --- 缺点: Barrier同步;任何算子发生故障，都需要发生全局停止和状态回滚; 拓扑越大，对性能的潜在影响越大.
   *
   * 3、End-TO-End Exactly Once
   * Flink使用了一种轻量级快照机制 —————— checkpoint来保证exactly once语义.
   * 所有任务的状态，在某个时间点的一份快照。这个时间点，应该是所有任务都恰好处理完同一个相同的输入数据的时候.
   * 【--- 应用状态的一致检查点，是Flink故障恢复机制的核心. ---】
   *
   * 【End-To-End Exactly Once的实现】
   * 1、内部保证   依赖checkpoint的机制保证;
   * 2、source端  发生故障时，可重设数据的读取位置;
   * 3、Sink端    从故障恢复时，数据不会重复写入外部系统.
   * --- 幂等写入 : 任意多次向一个系统写入数据，只对目标系统产生一次结果影响; HBase、Redis、Cassandra
   * --- 事务写入 : Flink先将带输出的数据保存下来暂时不写到外部系统，等待checkpoint结束的时刻，Flink上下游所有算子的数据
   * ------ 都是一致时，将之前保存的数据全部提交（Commit）到外部系统。【经过Checkpoint确认的数据才向外部系统写入。】
   * 在事务写的具体体现上，Flink目前提供了2种方式:
   * 1、预写日志。 WAL Write-Ahead-Log.
   * --- 通用性更强，适合几乎所有外部系统，但不能提供 100%的端到端的Exactly Once.
   * 2、两阶段提交。2PC  Two-Phase-Commit.
   * --- 如果外部系统本身就支持事务，比如Kafka，可以使用2PC方式，提供 100% 的端到端的Exactly Once.
   * 注意:
   * 事务写的方式能提供端到端的Exactly Once一致性，代价也是十分明显的，就是牺牲了实时性.
   * 数据数据不再是实时写入到外部系统，而是分批次的提交。
   * 【目前来说，没有完美的故障恢复和Exactly-Once保障机制，对于开发者来说，需要在不同需求之间权衡】
   *
   * ----------------------------------------------------------------
   * 【2PC的实现】
   * 对于每个Checkpoint，Sink任务会启动一个事务，并将接下来所有接收的数据添加到事务里.
   * 然后将这些数据写入外部Sink系统，但是提交它们——————此时是“预提交”。
   * 当接收到Checkpoint完成的通知时，才正式提交事务，实现结果的真正写入.
   * 这种方式真正实现了exactly-once，需要一个提供事务支持的外部sink系统.
   * Flink本身提供了TwoPhaseCommitSinkFunction接口.
   * 注意:
   * 1、外部sink系统必须支持事务，或者sink任务必须能模拟外部系统上的事务;
   * 2、在Checkpoint的间隔期间，必须能开启一个事务并接受数据写入;
   * 3、收到Checkpoint完成的通知之前，事务必须是【"等待提交"】状态，在故障恢复的情况下，可能需要一些时间.
   * --- 如果这个时候Sink系统关闭事务（比如超时了），那么未提交的数据就会丢失;
   * 4、Sink任务必须能在进程失败后恢复事务;
   * 5、提交事务必须是幂等操作.
   *
   * ----------------------------------------------------------------
   * 【Flink实现Kafka的End-To-End Exactly Once】
   * Flink1.4版本之前，支持Exactly Once语义，仅限于应用内部;
   * Flink1.4版本之后，通过2PC（TwoPhaseCommitSinkFunction）支持端到端的Exactly Once，且要求kafka 0.11+
   * 利用TwoPhaseCommitSinkFunction是通用的管理方案，只要实现对应的接口，且Sink的存储支持变乱提交，即可实现.
   *
   * 端到端的状态一致性的实现，需要每个组件都实现，对于Flink+Kafka的Pipeline系统来说:
   * 1、内部   利用Checkpoint机制，把状态存盘，发生故障的时候可以恢复，保证内部的状态一致性;
   * 2、source   kafka consumer作为Source，可以把offsets保存下来，如果后续任务出现了故障，
   * --- 恢复的时候可以由connector重置偏移量，重新消费数据，保证一致性.
   * FLink在消费Kafka的诗句时，在恢复状态时不会使用kafka自己维护的Offsets。假设:
   * 【使用kafka自己维护的offset，当从kafka消费数据没有处理完时flink出现了故障，恢复状态从kafka维护的offset消费，会丢失flink未处理的数据】
   * 3、Sink   kafka producer作为Sink，采用2PC提交Sink，需要实现一个TwoParallelSourceFunction.
   *
   * ----------------------------------------------------------------
   * 【Source和Sink具体怎么运行的？】
   * 1、JobManager 协调各个TaskManager进行Checkpoint存储;
   * Checkpoint保存在StateBackend中，默认StateBackend是内存级的，也可改为文奸级的进行持久化存储.
   *
   * 2、当Checkpoint启动时，JobManager会将Checkpoint的Barrier注入数据流;
   * Barrier会在算子间传递下去;
   *
   * 3、每个算子对当前状态做快照，保存到状态后端;
   * Checkpoint机制可保证内部的状态一致性.
   *
   * 4、每个内部的Transformation任务遇到Barrier时，都会把状态保存到Checkpoint中;
   * Sink任务首先把数据写入到外部kafka，这些数据都属于预提交的事务，遇到Barrier时，把状态保存到状态后端，并开启新的预提交事务.
   *
   * 5、当所有算子任务的快照完成，也就是这次的Checkpoint完成时，JobManager会向所有任务发通知，确认这次Checkpoint完成；
   * 执行过程其实是一个2PC，每个算子执行完成，会进行“预提交”，直到执行完sink操作，才发起“确认提交”，如果执行失败，预提交会放弃掉。
   *
   * 【2PC完整流程】
   * 1、第一条数据来了之后开启一个kafka的事务（Transaction），正常写入Kafka分区日志并标记为未提交，这就是“预提交”;
   * 2、JobManager触发Checkpoint操作，Barrier从Source开始向下传递，各个算子将状态存入StateBackend，并通知JobManager;
   * 3、Sink连接器收到Barrier，保存当前State，存入Checkpoint，通知JobManager，开启下一阶段的事务，用于提交下个Checkpoint的数据;
   * 4、JobManager收到所有任务的通知，发出确认信息，表示Checkpoint完成Sink收到JobManager的确认信息，正式提交这段时间的数据;
   * 5、外部 Kafka 关闭事务，提交的数据可以正常消费.
   * 因此，所有Operator必须对Checkpoint组中结果打成共识: 即所有Operator都必须认定数据提交要么成功执行，要么被终止然后回滚.
   *
   * kafka producer的容错: kafka 0.9 and 0.10 可以提供at-least-once语义.
   * 还要配置:
   * setLogFailuresOnly(false)
   * setFlushOnCheckpoint(true)
   * 建议修改kafka producer的重试次数，retries默认是0
   *
   * kafka producer的容错: kafka 0.11 就可以提供exactly-once语义.
   * 但是需要选择具体的语义:
   * Semantic.NONE
   * Semantic.AT_LEAST_ONCE【默认】
   * Semantic.EXACTLY_ONCE
   *
   * @param senv
   */
  def end2EndExactlyOnceTest(senv: StreamExecutionEnvironment): Unit = {
    import org.apache.flink.api.scala._

    // checkpoint 配置.
    senv.enableCheckpointing(5000)
    senv.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    senv.getCheckpointConfig.setMinPauseBetweenCheckpoints(500)
    senv.getCheckpointConfig.setCheckpointTimeout(60000)
    senv.getCheckpointConfig.setMaxConcurrentCheckpoints(1)

    senv.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

    val socketStream = senv.socketTextStream("hiwes", 9999)
    val topic = "test2"
    val prop = new Properties()
    prop.setProperty("bootstrap.servers", "hiwes:9092")
    prop.setProperty("transaction.timeout.ms", (60000 * 15).toString)

    // 使用至少一次语义
    //    val myProducer = new FlinkKafkaProducer011[String](topic, new SimpleStringSchema(), prop)

    // 使用Exactly once语义
    val myProducer = new FlinkKafkaProducer011[String](topic,
      new KeyedSerializationSchemaWrapper[String](new SimpleStringSchema()),
      prop,
      FlinkKafkaProducer011.Semantic.EXACTLY_ONCE
    )

    socketStream.addSink(myProducer)

    senv.execute("StreamingKafkaSinkScala")
  }

  /**
   * 8.2 MySQL来实现端到端Exactly Once语义.
   * 见类: StreamDemoKafka2Mysql
   */

  /**
   * 9.Flink的ProcessFunction API.
   * Flink的API分为3层:
   * --- SQL/Table API
   * --- DataStream/DataSet API
   * --- ProcessFunction(event, state, time)
   *
   * 之前的高层算子，是无法访问事件的时间戳信息和watermark信息的。但是在一些场景下非常重要.
   * 比如: MapFunction这样的map转换算子，无法访问时间戳或当前事件的EventTime.
   * 所以DataStream API提供了一些列Low-Level转换算子。可以访问【时间戳、watermark以及注册定时事件】
   * 还可以输出特定的一些事件，比如超时事件等.
   *
   * ProcessFunction是一个低阶的流处理操作，可以访问流处理程序的基础构建模块。
   * 1、事件Event
   * 2、状态State(容错性、一致性、仅在KeyedStream中)
   * 3、定时器（Timers）（EventTime和PeocessingTime，仅在KeyedStream中）
   * Flink提供了8个ProcessFunction:
   * 1、ProcessFunction DataSTream.
   * 2、KeyedProcessFunction 用于keyBy()之后的keyedStream.
   * 3、CoProcessFunction 用于connect连接的流.
   * 4、ProcessJoinFunction 用于join流操作.
   * 5、BroadcastProcessFunction 用于广播.
   * 6、KeyedBroadcastProcessFunction 用于KeyBy()之后的广播.
   * 7、ProcessWindowFunction 窗口增量聚合.
   * 8、ProcessAllWindowFunction 全窗口聚合.
   *
   * ProcessFunction 可以看做是一个具有keyed state和timers访问权限的FlatMapFunction。
   * 1、通过RuntimeContext访问keyed State;
   * 2、计时器Timer允许app对处理时间和事件时间的更改做出响应，对processElement()函数的每次调用
   * --- 都获得一个Context对象，可以访问元素的event time tiestamp 和TimerService;
   * 3、TimerService可用于为将来的event/process time注册回调，当定时器的达到定时时间时，会调用onTimer().
   *
   * 【keyedProcessFunction————重点】
   * 在其onTimer()中提供了对定时器对应key的访问，用来操作keyedStream。
   * 所有的ProcessFunction都继承自RichFunction接口，所以都有：
   * open()
   * close()
   * getRuntimeContext() 等方法，keyedProcessFunction还额外提供了2个方法:
   * 1、processElement(v:IN, ctx:Context, out:Collector[Out])
   * --- 流中每个元素都调用这个方法，结果会放在Collector中输出.Context可以访问元素的时间戳，元素的key，TimerService时间服务.
   * --- Context还可将结果输出到别的流（side outputs）
   * 2、onTimer(timestamp:Long, ctx:OnTimeContext, out:Collector[Out])
   * --- 一个回调函数。提供上下文信息.
   *
   * 【TimerService和Timers定时器】
   * Context和OnTimerContext所持有的TimerService对象拥有以下方法:
   * 1、currentProcessingTime():Long 返回当前处理时间.
   * 2、currentWatermark():Long 返回当前watermark时间戳.
   * 3、registerProcessingTimeTimer(timestamp:Long):Unit 注册当前key的processing time的定时器，当处理时间到达定时时间时，触发timer.
   * 4、registerEventTimeTimer(timestamp:Long):Unit 注册当前key的eventtime定时器。当watermark >= 定时器注册的时间时，触发定时器执行回调函数.
   * 5、deleteProcessingTimeTimer(timestamp:Long):Unit 删除之前注册处理时间定时器，如果没有这个时间戳的定时器，则不执行.
   * 6、deleteEventTimeTimer(timestamp:Long):Unit 删除之前注册的事件时间定时器，如果没有这个时间戳的定时器，则不执行.
   *
   * 当定时器Timer触发时，会执行回调函数onTimer()。注意定时器只能在keyedStream上使用.
   *
   * 以下实例为:
   * 实时监控服务器机架的温度，一定时间内超过阈值，且后一次上报的温度超过前一次的温度，则触发告警（温度持续升高中）
   */
  def systemMonitoringTest(senv: StreamExecutionEnvironment): Unit = {
    import org.apache.flink.api.scala._

    // 设置事件时间处理数据.
    senv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 并行度不指定，默认线程数和当前环境的线程数有关（8）
    senv.setParallelism(1)

    // 开启checkpoint
    senv.enableCheckpointing(60000)
    senv.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE)
    senv.getCheckpointConfig.setCheckpointTimeout(100000)
    senv.getCheckpointConfig.setFailOnCheckpointingErrors(false)
    senv.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    senv.getCheckpointConfig.setMinPauseBetweenCheckpoints(100)

    senv.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION)
    senv.setRestartStrategy(RestartStrategies.failureRateRestart(3,
      org.apache.flink.api.common.time.Time.seconds(300),
      org.apache.flink.api.common.time.Time.seconds(10)))

    // 接入数据.
    val socketStream = senv.socketTextStream("hiwes", 9999)

    // 字符串转换为温度事件对象.
    val watermarksDataStream = socketStream.map(new MapFunction[String, TemperatureEvent] {
      override def map(value: String): TemperatureEvent = {
        val fields = value.split(",")
        TemperatureEvent(fields(0).toInt, fields(1).toDouble, fields(2).toLong)
      }
    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[TemperatureEvent](Time.seconds(0)) {
      override def extractTimestamp(element: TemperatureEvent): Long = {
        element.timestamp
      }
    })

    watermarksDataStream.print("上报数据 >>> ")

    watermarksDataStream.keyBy("rackID")
      .process(new TemperatureWarningProcess())
      .printToErr("警告数据 >>> ")

    senv.execute()
  }

  // 警告事件处理.
  class TemperatureWarningProcess extends KeyedProcessFunction[Tuple, TemperatureEvent, Tuple2[Int, String]] {
    var temperatureList: ListState[TemperatureEvent] = null

    override def open(parameters: Configuration): Unit = {
      super.open(parameters)
      temperatureList = getRuntimeContext.getListState(new ListStateDescriptor[TemperatureEvent]("" +
        "tempList",
        TypeInformation.of(new TypeHint[TemperatureEvent] {})))
    }

    override def processElement(value: TemperatureEvent, ctx: KeyedProcessFunction[Tuple, TemperatureEvent, (Int, String)]#Context, out: Collector[(Int, String)]): Unit = {
      // 取出上一次的温度值
      var lasTemperatureEvent: TemperatureEvent = null
      val temperatureWarings: lang.Iterable[TemperatureEvent] = temperatureList.get()

      val iterator: util.Iterator[TemperatureEvent] = temperatureWarings.iterator()

      while (iterator.hasNext) {
        lasTemperatureEvent = iterator.next()
      }

      // 如果是第一条数据
      if (lasTemperatureEvent == null)
        lasTemperatureEvent = new TemperatureEvent(0, 0, 0)

      // 如果温度超过阈值，则将事件数据放入状态列表中.
      //      if (value.temperature >= TEMPPERATURE_THEWHOLD && value.temperature > lasTemperatureEvent) {
      if (value.temperature >= 60.0d && value.temperature > lasTemperatureEvent.temperature) {
        // 将当前温度数据添加到温度状态集合中
        val temperatureWarning = TemperatureEvent(value.rackID, value.temperature, value.timestamp)
        temperatureList.add(temperatureWarning)

        // 如果定时器的时间戳已被初始化，则重新绑定定时器
        if (lasTemperatureEvent.timestamp == 0) {
          val timerTs = ctx.timerService().currentProcessingTime() + 5000L
          ctx.timerService().registerProcessingTimeTimer(timerTs)
        }

      } else {
        // 如果没有温度警告的温度数据或当前数据的温度低于温度阈值，则取消定时器
        if (value.temperature > lasTemperatureEvent.temperature) {
          // 如果温度下降，就取消定时器
          ctx.timerService().deleteProcessingTimeTimer(lasTemperatureEvent.timestamp)
          // 清空警告的数据
          temperatureList.clear()
        }
      }
    }

    // 5s后定时器触发，输出报警.
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, TemperatureEvent, (Int, String)]#OnTimerContext, out: Collector[(Int, String)]): Unit = {
      val temperatureEvents = temperatureList.get()
      val iterator = temperatureEvents.iterator()

      val list = new util.ArrayList[TemperatureEvent]()
      while (iterator.hasNext) {
        list.add(iterator.next())
      }
      if (list.size() > 1) {
        out.collect((list.get(0).rackID, "五秒内上报温度连续两次超过阈值，且温度在持续升高中"))
      }
      temperatureList.clear()

    }
  }

  case class TemperatureEvent(rackID: Int, temperature: Double, timestamp: Long)

}
