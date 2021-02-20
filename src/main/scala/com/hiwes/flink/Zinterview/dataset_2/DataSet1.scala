package com.hiwes.flink.Zinterview.dataset_2

import java.io.{BufferedReader, FileReader}
import java.{lang, util}
import java.text.SimpleDateFormat

import org.apache.flink.api.common.accumulators.{IntCounter, LongCounter}
import org.apache.flink.api.common.functions.{GroupReduceFunction, RichFlatMapFunction, RichGroupReduceFunction, RichMapFunction, RichReduceFunction}
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.java.aggregation.Aggregations
import org.apache.flink.api.java.io.CsvReader
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.util.Collector

import scala.collection.mutable

/**
 * DataSet API.
 *
 * @by hiwes since 2021/02/19
 */
object DataSet1 {
  def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    // 1.基于本地集合创建DataSet
    //    val data = createDataSetByLocal(env).setParallelism(1)
    //    data.getParallelism // 2.获取指定算子并行度

    // 3.基于文件创建Source
    //    createDataSetByCSVFile(env)
    //    traverseDirectory(env)

    // 4.DataSet的Transformation.
    //    transformations4DataSet(env)

    // 5.DataSet的Sinks.
    //    sinks4DataSet(env)

    // 6.DataSet的广播变量.
    //    broadcast4DataSet(env)

    // 7.DataSet的累加器和计数器.
    //    accumulatorsAndCounters4DataSet(env)

    // 8.DataSet的分布式缓存.
    distributedCache4DataSet(env)

  }

  /**
   * 1.基于本地集合创建DataSet
   */
  def createDataSetByLocal(env: ExecutionEnvironment): DataSet[Long] = {
    import org.apache.flink.api.scala._

    //    env.fromElements()  // 基于复合形式如Tuple、自定义对象
    env.fromElements("haha", "heihei").print()
    env.fromElements("haha", 1)

    //    env.fromCollection()  // 基于Collection
    env.fromCollection(Array("haha", "heihei"))
    env.fromCollection(Set("lalala", "guaguagua"))
    env.fromCollection(mutable.Queue("Spark", "Hadoop"))

    //    env.generateSequence()  // 基于Sequence
    env.generateSequence(1, 10)
  }

  /**
   * 3.基于文件创建Source
   * 本地文件数据
   * HDFS文件数据
   * CSV文件数据
   * 压缩文件
   * 遍历目录
   */
  def createDataSetByFile(env: ExecutionEnvironment): Unit = {
    import org.apache.flink.api.scala._

    // 3.1 本地文件数据
    createDataSetByLocalFile(env)

    // 3.3 CSV文件数据
    createDataSetByHDFSFile(env)
    // 3.4 压缩文件
    createDataSetByGZFile(env)

    // 3.5 遍历目录
    traverseDirectory(env)
  }

  // 3.1 本地文件数据
  def createDataSetByLocalFile(env: ExecutionEnvironment): Unit = {
    env.readTextFile("file:///Users/hiwes/data/test1.txt")
      .print()
  }

  // 3.2 HDFS文件数据
  def createDataSetByHDFSFile(env: ExecutionEnvironment): Unit = {
    env.readTextFile("hdfs://hiwes:8020/test/LICENSE-2.0.txt")
  }

  // 3.3 CSV文件数据
  // fieldLimiter 设置分隔符，默认“,”
  // ignoreFirstLine 忽略第一行
  // includeFields 设置选取哪几列
  // pojoType 和后面字段名就是对应列。
  def createDataSetByCSVFile(env: ExecutionEnvironment): Unit = {
    import org.apache.flink.api.scala._
    //   def readCsvFile[T : ClassTag : TypeInformation](
    //      filePath: String,
    //      lineDelimiter: String = "\n",
    //      fieldDelimiter: String = ",",
    //      quoteCharacter: Character = null,
    //      ignoreFirstLine: Boolean = false,
    //      ignoreComments: String = null,
    //      lenient: Boolean = false,
    //      includedFields: Array[Int] = null,
    //      pojoFields: Array[String] = null): DataSet[T] = {
    env.readCsvFile[Tuple3[String, Int, String]]("/Users/hiwes/data/people.csv"
      , "\n", ",", null, ignoreFirstLine = true, null, false)
      .print() // 读取CSV文件需要进行相应类型的对应，在readCsvFile后跟类型.

    // 或者直接使用POJO对象进行操作也可,但是注意其他参数是不可省略的.
    env.readCsvFile[JOB]("/Users/hiwes/data/people.csv"
      , "\n", ",", null, ignoreFirstLine = true, null, false)
      .print()
  }

  case class JOB(name: String, age: Int, job: String)

  // 3.4 压缩文件
  def createDataSetByGZFile(env: ExecutionEnvironment): Unit = {
    // GZIP   .gz .gzip
    // Bzip2  .bz2
    // XZ     .xz
    env.readTextFile("file:///Users/hiwes/data/test.gz")
      .print()
  }

  // 3.5 遍历目录
  def traverseDirectory(env: ExecutionEnvironment): Unit = {
    // 生成配置参数.
    val param: Configuration = new Configuration()
    param.setBoolean("recursive.file.enumeration", true)

    // 读取目录配合递归
    env.readTextFile("/Users/hiwes/data").withParameters(param)
      .print()

  }

  /**
   * 4.DataSet的Transformation.
   * map          将DataSet中每个元素转换为另一个元素.
   * flatMap      将DataSet中每个元素转换为0.....n个元素.
   * filter       过滤出符合条件的元素.
   * reduce       对一个DataSet或一个Group做聚合计算，最终聚合为一个元素.
   * reduceGroup  对一个DataSet或一个Group做聚合计算，最终聚合为一个元素。
   * ---和reduce的区别: groupBy函数会将一个个单词进行分组，然后被reduce一个个拉取过来，
   * ------在数据量大的情况下，会拉去很多的数据，增加了网络IO，reduceGroup是reduce的一种优化方案，
   * ------会先分组reduce，然后做整体的reduce，可以很好地减少网络IO.
   * aggregate    按内置的方式实现聚合，如: SUM / MIN / MAX
   * minBy和maxBy 获取指定字段的最小值、最大值.
   * ---和aggregate的区别: min方法只能用于元组，minBy可以用于集合数据DataSet
   * ------而且计算逻辑也不同:
   * ------min在计算过程中，会记录最小值，对于其他列会取最后一次出现的，然后和最小值组合形成结果返回。
   * ------minBy在计算过程中，当遇到最小值，会将第一次出现的最小值所在的整个元素返回.
   * distinct     去除重复的数据.
   * join         连接两个DataSet.【多用于Stream中】
   * union        合并多个DataSet, 合并的DataSet类型必须一致.
   * rebalance    数据倾斜问题的较好的解决方式.
   * partitionByHash    按指定key进行hash分区.
   * sortPartition      按指定字段值进行分区排序. sortPartition(firld, order)
   *
   * @param env
   */
  def transformations4DataSet(env: ExecutionEnvironment): Unit = {
    import org.apache.flink.api.scala._
    // 使用map操作，读取apache.log文件中的字符串数据转换成ApacheLogEvent对象
    //    val data: DataSet[String] = env.readTextFile("file:///Users/hiwes/data/apache.log")
    //    data.map(_.toLowerCase())
    //      .map(x => {
    //        val arr = x.split(" ")
    //        val simpledataFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
    //        ApacheLogEvent(arr(0), arr(1).toInt, simpledataFormat.parse(arr(2)).getTime, arr(3), arr(4))
    //      })
    //      .print()

    // 读取flatmap.log文件中的数据,使用flatMap进行转换.
    //    val data = env.readTextFile("file:///Users/hiwes/data/flatMap.log")
    //    data.flatMap(new RichFlatMapFunction[String, String] {
    //      override def flatMap(value: String, out: Collector[String]): Unit = {
    //        val arr = value.split(",")
    //        out.collect(arr(0) + " 有 " + arr(1))
    //        out.collect(arr(0) + " 有 " + arr(2))
    //        out.collect(arr(0) + " 有 " + arr(3))
    //      }
    //    }).print()

    /**
     * 修改后的写法，篇幅更短更简便.
     */
    //    data.flatMap((value: String, out: Collector[String]) => {
    //      val arr = value.split(",")
    //      out.collect(arr(0) + " 有 " + arr(1))
    //      out.collect(arr(0) + " 有 " + arr(2))
    //      out.collect(arr(0) + " 有 " + arr(3))
    //    }).print()


    // 使用filter进行过滤
    //    data.filter(_.nonEmpty).print()

    // 读取apache.log文件，统计IP地址访问pv数量，使用reduce操作聚合为一个最终结果.
    //    val data = env.readTextFile("file:///Users/hiwes/data/apache.log")
    //    data.map(x => (x.toLowerCase().split(" ")(0), 1))
    //      .groupBy(0)
    //      .reduce(new RichReduceFunction[(String, Int), (String, Int)]() {
    //        override def reduce(value1: (String, Int), value2: (String, Int)): (String, Int) = {
    //          (value1._1, value1._2 + value2._2)
    //        }
    //      })
    //    data.map(x => (x.toLowerCase().split(" ")(0), 1))
    //      .groupBy(0)
    //      .reduce((x,y) => (x._1,x._2+y._2))

    // 读取apache.log文件，统计IP地址访问pv数量，使用reduceGroup操作聚合为一个最终结果.
    //    val data = env.readTextFile("file:///Users/hiwes/data/apache.log")
    //      .map(x => (x.toLowerCase().split(" ")(0), 1))
    //      .groupBy(0)
    //
    //    data.reduceGroup(new RichGroupReduceFunction[(String, Integer), (String, Integer)] {
    //      override def reduce(values: lang.Iterable[(String, Integer)], out: Collector[(String, Integer)]): Unit = {
    //        var key: String = null
    //        var count = 0
    //        val itr = values.iterator()
    //        while (itr.hasNext) {
    //          val value = itr.next()
    //          key = value._1
    //          count += value._2
    //        }
    //        out.collect((key, count))
    //      }
    //    })

    /**
     * 注意，上面的写法会报错，原因暂时未知，所以在使用reduceGroup的使用，尽量使用下面的写法，使用Int代替Integer.
     */
    //    data.reduceGroup(new RichGroupReduceFunction[(String, Int), (String, Int)] {
    //      override def reduce(values: lang.Iterable[(String, Int)], out: Collector[(String, Int)]): Unit = {
    //        var key: String = null
    //        var count = 0
    //        val itr = values.iterator()
    //        while (itr.hasNext) {
    //          val value = itr.next()
    //          key = value._1
    //          count += value._2
    //        }
    //        out.collect((key, count))
    //      }
    //    })

    /**
     * 或者直接使用下面的方法，直接对values和out进行处理.
     */
    //    data.reduceGroup((values: Iterator[(String, Int)], out: Collector[(String, Int)]) => {
    //      var key: String = null
    //      var count = 0
    //      for (value <- values) {
    //        key = value._1
    //        count += value._2
    //      }
    //      out.collect((key, count))
    //    }).print()

    // 读取apache.log日志，统计ip地址访问pv数量，使用aggregate操作进行PV访问统计.
    /**
     * 注意，aggregate只能用于元组
     */
    //    val data = env.readTextFile("file:///Users/hiwes/data/apache.log")
    //    val reduceGroupSource = data.map(x => (x.toLowerCase().split(" ")(0), 1))
    //      .groupBy(0)
    //      .reduceGroup(
    //        (values: Iterator[(String, Int)], out: Collector[(String, Int)]) => {
    //          var key: String = null
    //          var count = 0
    //          for (value <- values) {
    //            key = value._1
    //            count += value._2
    //          }
    //          out.collect((key, count))
    //        })
    //
    //    val aggregationMax = reduceGroupSource.aggregate(Aggregations.MAX, 1)
    //    val aggregationMin = reduceGroupSource.aggregate(Aggregations.MIN, 1)
    //    val aggregationSum = reduceGroupSource.aggregate(Aggregations.SUM, 1)
    //
    //    aggregationMax.print() //7
    //    aggregationMin.print() // 1
    //    aggregationSum.print() // 14

    /**
     * aggregate的简便写法.直接写max、min、sum(1)即可.
     */
    //    reduceGroupSource.max(1).print()
    //    reduceGroupSource.min(1).print()
    //    reduceGroupSource.sum(1).print()

    // 读取apache.log日志，统计ip地址访问pv数量，使用minBy、maxBy操作进行pv访问量统计.
    //    val data = env.readTextFile("file:///Users/hiwes/data/apache.log")
    //    val reduced = data.map(x => (x.toLowerCase().split(" ")(0), 1))
    //      .groupBy(0)
    //      .reduceGroup(
    //        (values: Iterator[(String, Int)], out: Collector[(String, Integer)]) => {
    //          var key: String = null
    //          var count = 0
    //          for (value <- values) {
    //            key = value._1
    //            count += value._2
    //          }
    //          out.collect((key, count))
    //        })
    //
    //    reduced.minBy(1).print()
    //    reduced.maxBy(1).print()

    /**
     * Min在计算的过程中，会记录最小值，对于其它的列，会取最后一次出现的，然后和最小值组合形成结果返回
     * minBy在计算的过程中，当遇到最小值后，将第一次出现的最小值所在的整个元素返回。
     */
    // 读取apache.log日志，统计有哪些ip访问了网站
    //    val data = env.readTextFile("file:///Users/hiwes/data/apache.log")
    //    data.map(new RichMapFunction[String, Tuple1[String]]() {
    //      override def map(value: String): Tuple1[String] = {
    //        Tuple1(value.split(" ")(0))
    //      }
    //    }).distinct(0).print()
    /**
     * distinct只能用于tuple类型.
     */

    // 将两个csv文件进行join，然后打印出来.
    //    val subject = env.readCsvFile[Subject]("file:///Users/hiwes/data/subject.csv"
    //      , "\n", ",", null, ignoreFirstLine = true, null, false)
    //
    //    val score = env.readCsvFile[Score]("file:///Users/hiwes/data/score.csv"
    //      , "\n", ",", null, ignoreFirstLine = true, null, false)
    //
    //    val end: JoinDataSet[Score, Subject] = score.join(subject).where("subjectId").equalTo("id")
    //    end.print()
    /**
     * join需要在readCsvFile后加入具体POJO类型.
     */

    // 将下列数据取并集
    //    // "hadoop","hive","flume"
    //    // "hadoop","hive","spark"
    //    val data1 = env.fromElements("hadoop","hive","flume")
    //    val data2 = env.fromElements("hadoop","hive","spark")
    //    data1.union(data2).print()
    /**
     * 【Rebalance】
     * 用来解决数据倾斜问题的很好的方式.
     * 测试不使用和使用rebalance的性能差别.
     */
    // 不使用Rebalance.
    //    neverUseRebalance(env)
    // 使用rebalance.
    //    useRebalance(env)

    /**
     * 分区.
     * 1.按照指定key进行hash分区.       partitionByHash
     * ------分区数量和并行度有关，如果不设置并行度，会自动根据内容设置分区数量
     * ------还有一个同类函数: partitionByRange, 按照key的范围进行排序.
     * ------hash和range是flink自行控制，开发者无法控制，其中:
     * ------hash规则是一样的key放入一个分区
     * ------range规则是值范围在一个区域内(接近)的key，在一个分区.
     * 2.根据指定的字段值进行分区的排序.   sortPartition(field, order)
     * ------
     */
    // 1.partitionByHash
    //    usePartitionByHash(env)

    // 2.sortPartition
    //    useSortPartition(env)

  }

  // 使用sortPartition,根据指定字段值进行分区的排序
  def useSortPartition(env: ExecutionEnvironment): Unit = {
    import org.apache.flink.api.scala._
    val data: DataSet[(String, Int)] = env.fromElements(
      ("hadoop", 11),
      ("hadoop", 21),
      ("hadoop", 3),
      ("hadoop", 16),
      ("hive", 13),
      ("hive", 31),
      ("hive", 21),
      ("hive", 11),
      ("hive", 15),
      ("hive", 19),
      ("spark", 51),
      ("spark", 61),
      ("spark", 19),
      ("spark", 35),
      ("spark", 66),
      ("spark", 76),
      ("flink", 11),
      ("flink", 51),
      ("flink", 31)
    )

    // 仅按照单词排序
    val sorted1: DataSet[(String, Int)] = data.sortPartition(0, Order.ASCENDING)

    sorted1.map(new RichMapFunction[(String, Int), (Int, (String, Int))] {
      override def map(value: (String, Int)): (Int, (String, Int)) = {
        (getRuntimeContext.getIndexOfThisSubtask, value)
      }
    }).print()

    // 在分区内部按单词排序
    val sorted2 = data.partitionByHash(0).sortPartition(0, Order.ASCENDING)
    sorted2.map(new RichMapFunction[(String, Int), (Int, (String, Int))] {
      override def map(value: (String, Int)): (Int, (String, Int)) = {
        (getRuntimeContext.getIndexOfThisSubtask, value)
      }
    }).print()

    // 在分区内按单词和数字排序
    val sorted3 = data.partitionByHash(0).sortPartition(0, Order.ASCENDING).sortPartition(1, Order.ASCENDING)
    sorted3.map(new RichMapFunction[(String, Int), (Int, (String, Int))] {
      override def map(value: (String, Int)): (Int, (String, Int)) = {
        (getRuntimeContext.getIndexOfThisSubtask, value)
      }
    }).print()
  }

  // 使用partitionByHash,根据key进行hash分区.
  def usePartitionByHash(env: ExecutionEnvironment): Unit = {
    import org.apache.flink.api.scala._
    val data = env.fromElements(
      Tuple2(1, 1),
      Tuple2(2, 1),
      Tuple2(3, 1),
      Tuple2(1, 1),
      Tuple2(2, 1),
      Tuple2(3, 1),
      Tuple2(1, 1),
      Tuple2(2, 1),
      Tuple2(3, 1),
      Tuple2(1, 1),
      Tuple2(4, 1),
      Tuple2(5, 1)
    )
    val partitionOperator: DataSet[(Int, Int)] = data.partitionByHash(0) // 根据指定key，进行hash分区，key相同，分到一起.
    val mapOperator = partitionOperator.map(new RichMapFunction[(Int, Int), (Int, (Int, Int))] {
      override def map(value: (Int, Int)): (Int, (Int, Int)) = {
        (getRuntimeContext.getIndexOfThisSubtask, value)
      }
    })
    mapOperator.print()
  }

  // 不使用Rebalance
  def neverUseRebalance(env: ExecutionEnvironment): Unit = {
    import org.apache.flink.api.scala._
    env.generateSequence(0, 100).filter(_ > 8)
      .map(new RichMapFunction[Long, Tuple2[Integer, Long]] {
        override def map(value: Long): (Integer, Long) = {
          Tuple2(getRuntimeContext.getIndexOfThisSubtask, value)
        }
      })
      .print()
  }

  // 使用Rebalance
  /**
   * 使用Rebalance之后，相当于一个shuffle的过程，将数据在所有分区上变得均匀.
   *
   * @param env
   */
  def useRebalance(env: ExecutionEnvironment): Unit = {
    import org.apache.flink.api.scala._
    env.generateSequence(0, 100).filter(_ > 8).rebalance() // 使用Rebalance.
      .map(new RichMapFunction[Long, Tuple2[Integer, Long]] {
        override def map(value: Long): (Integer, Long) = {
          Tuple2(getRuntimeContext.getIndexOfThisSubtask, value)
        }
      })
      .print()
  }

  case class Score(id: Int, name: String, subjectId: Int, score: Double)

  case class Subject(id: Int, name: String)

  case class ApacheLogEvent(ip: String, userId: Int, timestamp: Long, method: String, path: String)

  /**
   * 5.DataSet的Sinks.
   * 基于本地集合的Sink  ||  基于文件的Sink
   *
   * @param env
   */
  def sinks4DataSet(env: ExecutionEnvironment): Unit = {
    // 基于本地集合的Sink
    localOutput(env)

    // 基于文件的Sink
    fileOutput(env)
  }

  // 基于本地集合的Sink
  def localOutput(env: ExecutionEnvironment): Unit = {
    val data = env.generateSequence(1, 10)

    // local print
    data.print()
    print("----------------")

    // print to local stderr
    data.printToErr()
    print("----------------")

    // collect to local collection
    val collect: Seq[Long] = data.collect()
    println(collect)

    /**
     * stdout 标准输出设备.
     * stderr 标准错误输出设备.
     * 二者都默认向屏幕输出，但如果用转向标准输出到磁盘文件可看到区别:
     * stdout输出到磁盘文件，stderr在屏幕.
     * 默认情况下，stdout是行缓冲的，其输出会放在一个buffer里面，只有到换行的时候，才输出到屏幕.
     * stderr是无缓冲的，会直接输出.
     * 【行缓冲】只有遇到换行符，才执行实际的IO操作，缓冲区满了也强制执行.
     * 【无缓冲】不缓存，直接进行IO操作.          换行符/满了——fflush——fclose
     * 【满缓冲】只有缓冲区满了才进行实际的IO操作.  满了——fflush——fclose
     */
  }

  // 基于文件的Sink
  def fileOutput(env: ExecutionEnvironment): Unit = {
    val data = env.generateSequence(1, 10)

    // sink to local fille
    data.writeAsText("file:///Users/hiwes/data/output/datasink", FileSystem.WriteMode.OVERWRITE)

    // sink to hdfs file
    data.writeAsText("hdfs://hiwes:8020/output/datasink", FileSystem.WriteMode.OVERWRITE).setParallelism(1)

    // has no flink task，so start flink task manually
    env.execute()
  }

  /**
   * 6.DataSet的广播变量.
   *
   * 创建后的广播变量，会被加载到TaskManager的内存中，从而在不同的task中都可以进行访问.
   * 注意不要对广播变量进行修改，因为这会使得数据不一致.
   * 另外广播变量不能太大，否则会出现OOM现象.
   * BroadCast: 通过withBroadcastSet(dataset, String)来注册.
   * Access:    通过getRuntimeContext()。getBroadcastVariable(String)来访问广播变量.
   * 广播后的变量，每个节点只存一份，否则每个task都会拷贝一份，造成内存资源浪费.
   *
   * @param env
   */
  def broadcast4DataSet(env: ExecutionEnvironment): Unit = {
    import org.apache.flink.api.scala._
    val studentInfoDataSet = env.fromElements(
      (1, "王大锤"),
      (2, "潇潇"),
      (3, "甜甜")
    )
    val scoreInfoDataSet = env.fromElements(
      (1, "数据结构", 99),
      (2, "英语", 100),
      (3, "C++", 96),
      (5, "Java", 97),
      (3, "Scala", 100)
    )

    // 注意，广播变量的使用分为2步——————发布和获取.
    scoreInfoDataSet.map(new RichMapFunction[(Int, String, Int), (String, String, Int)] {
      val map = new mutable.HashMap[Int, String]()

      // open方法，在实例化的开始会执行一次.
      override def open(parameters: Configuration): Unit = {
        val broadcastVariable: util.List[(Int, String)] = getRuntimeContext.getBroadcastVariable("student")
        val ite = broadcastVariable.iterator()
        while (ite.hasNext) {
          val stu = ite.next()
          this.map.put(stu._1, stu._2) // 至此，内部map进行了数据的充填.
        }
      }

      override def map(value: (Int, String, Int)): (String, String, Int) = {
        val stuId = value._1
        val stuName = this.map.getOrElse(stuId, "未知学生姓名")
        (stuName, value._2, value._3)
      }
    }).withBroadcastSet(studentInfoDataSet, "student")
      .print()

    /**
     * 使用broadcast的注意事项:
     * 1、在使用的时候需要留意，小数据才能进行广播，否则可能出现OOM问题;
     * 2、使用broadcast的时候可以根据不同的算子来进行获取，在每个Rich方法内部，都能得到重写后的override 方法，包括map和open，
     * ------其中open()是在实例化的时候会直接运行一次的方法，可以作为初始化使用.
     * 3、在open()方法内部进行外部广播变量的获取是允许的，所以在真正的map方法之前其实有很多东西已经进行处理.
     * 4、可以在map()算子的尾部跟withBroadcastSet(datset，"广播名").
     */

  }

  /**
   * 7.DataSet的累加器和计数器.
   *
   * Accumulator 累加器，和counter的引用场景差不多.
   * 可以在flink job任务中的算子函数中操作累加器，但只能在任务执行结束之后才能获得累加器最终结果.
   * Flink的内置累加器，都实现了Accumulator接口:
   * 1、IntCounter
   * 2、LongCounter
   * 3、DoubleCounter
   * 使用步骤:
   * 1、创建累加器
   * 2、注册累加器
   * 3、使用累加器
   * 4、获取累加器结果
   *
   * @param env
   */
  def accumulatorsAndCounters4DataSet(env: ExecutionEnvironment): Unit = {
    // 不使用累加器
    neverUseAccumulator(env)

    // 使用累加器
    useAccumulator(env)

    /**
     * Broadcast和Accumulator的区别:
     * Broadcast允许将一个只读的变量缓存在每台机器上，而不用在task之间传递变量，可以进行共享，但不能进行更改;
     * Accumulator可以在不同任务中对同一个变量进行累加操作（Flink自己实现了线程安全）,对分布式系统来说，不仅累加的各个Task会跨slot，甚至会跨机器进行累加.
     * 所以: 传统的累加不可用，只能用Flink提供的累加器才可以.
     */
  }

  // 7.1 不使用累加器
  def neverUseAccumulator(env: ExecutionEnvironment): Unit = {
    import org.apache.flink.api.scala._
    val data = env.generateSequence(1, 10)
    val map = data.map(new RichMapFunction[Long, Long] {

      var count = 0

      override def map(value: Long): Long = {
        this.count += 1;
        println("Thread id: " + getRuntimeContext.getIndexOfThisSubtask + " ,counter: " + count)
        value
      }
    })

    map.writeAsText("file:///Users/hiwes/data/output/accumulators", FileSystem.WriteMode.OVERWRITE)

    env.execute()

  }

  // 7.2 使用累加器
  def useAccumulator(env: ExecutionEnvironment): Unit = {
    import org.apache.flink.api.scala._
    val data = env.generateSequence(1, 10)

    val map = data.map(new RichMapFunction[Long, Long] {

      val accu = new IntCounter()

      override def open(parameters: Configuration): Unit = {
        super.open(parameters)
        getRuntimeContext.addAccumulator("sum", accu)
      }

      override def map(value: Long): Long = {
        accu.add(1)
        println("Thread id: " + getRuntimeContext.getIndexOfThisSubtask + " ,counter: " + accu.getLocalValue)
        value
      }
    })

    map.writeAsText("file:///Users/hiwes/data/output/accumulators", FileSystem.WriteMode.OVERWRITE)
    val jobExecutionResult = env.execute()
    val sum: Int = jobExecutionResult.getAccumulatorResult("sum")
    print(sum)
  }

  /**
   * 8.DataSet的分布式缓存.
   *
   * Flink提供的一个类似于Hadoop的分布式缓存，让并行运行实例的函数可以在本地访问.
   * 可以用来分享外部静态的数据，如: 机器学习的算法模型等.
   * 步骤:
   * 1、使用ExecutionEnvironment实例对本地或远程的文件，为缓存文件制定一个名词注册该缓存文件;
   * 2、当程序执行时，Flink会自动将复制文件或目录到所有worker节点的本地文件系统中.
   *
   * 和广播变量的区别:
   * 1、广播变量广播的是程序中的DataSet，分布式缓存广播的是文件.
   * 2、广播变量将数据广播到各个TaskManager的内存中，分布式缓存将数据广播到各个TaskManager的本地.
   * 使用:
   * 1、env.registerCachedFile 注册一个分布式缓存.
   * 2、使用getRuntimeContext.getDistributedCache.getFile(文件名) 获取分布式缓存.
   *
   * @param env
   */
  def distributedCache4DataSet(env: ExecutionEnvironment): Unit = {
    import org.apache.flink.api.scala._
    val scoreInfoDataSet = env.fromElements(
      (1, "数据结构", 99),
      (2, "英语", 100),
      (3, "C++", 96),
      (5, "Java", 97),
      (3, "Scala", 100)
    )
    // 注册分布式缓存.
    env.registerCachedFile("hdfs://hiwes:8020/input/distributed_student.txt", "student")

    // 通过map来组合数据并获取分布式缓存文件内容.
    scoreInfoDataSet.map(new RichMapFunction[(Int, String, Int), (String, String, Int)] {

      val map = new mutable.HashMap[Int, String]()

      override def open(parameters: Configuration): Unit = {
        try {
          super.open(parameters)
          val distributedFile = getRuntimeContext.getDistributedCache.getFile("student")
          val bufferedReader = new BufferedReader(new FileReader(distributedFile))
          var line: String = null
          while ((line = bufferedReader.readLine()) != null) {
            val elements = line.split(",")
            map.put(elements(0).toInt, elements(1))
          }
        } catch {
          case e: Exception => e.printStackTrace()
        }
      }

      override def map(value: (Int, String, Int)): (String, String, Int) = {
        (map.getOrElse(value._1, "未知学生姓名"), value._2, value._3)
      }
    }).print()

  }

  /**
   * 重点:
   * Flink里面的执行上下文Context,到底是什么.
   * Flink中的Context可以分为3种:
   * 1、StreamExecutionEnvironment.
   * ------flink程序运行前的上下文，包括【执行时配置ExecutionCofig如:parallelism、maxParallelism等，还有
   * ------CheckpointConfig如:checkpointTimeout、checkpointInterval等】。基本包括了Flink程序执行所需的一切配置.
   * 2、RuntimeContext.
   * ------flink程序运行中的上下文，每一个RichFunction都有一个RuntimeContext.【在使用RichFunction的时候进行的中间操作】
   * ------可以获得【getTaskName、getIndexOfThisSubtask、getIntCounter等】。基本是函数执行的上下文，累加器、计数器、分布式缓存等.
   * 3、函数专有的Context.
   * ------flink函数内的上下文，如当前处理element的时间戳或是firing timer的时间戳，正在处理的key等.
   */

}
