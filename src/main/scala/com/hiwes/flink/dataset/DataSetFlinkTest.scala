package com.hiwes.flink.dataset

import java.util

import com.hiwes.flink.cores.Person
import org.apache.flink.configuration.Configuration
import org.apache.flink.types.StringValue
import org.apache.hadoop.io.{IntWritable, Text}

/**
 * DataSet API实验类。
 *
 */
object DataSetFlinkTest {

  def main(args: Array[String]): Unit = {

    val parh0 = "file:///Users/hiwes/data/"
    val path = "file:///Users/hiwes/data/test1.txt"
    val path2 = "hdfs://hiwes:9092/data/test1.txt"
    val csvPath = "file://Users/hiwes/data/people.csv"
    import org.apache.flink.api.scala._

    val seq = Seq(1, 2, 3, 4, 5)
    val ite = seq.iterator
    val env = ExecutionEnvironment.getExecutionEnvironment

    // 1、从文件创建数据源
    val line: DataSet[String] = env.readTextFile(path) // 从文件获取数据源 // 甚至可以读取一个文件夹里的所有文件！！！
    val line2: DataSet[String] = env.readTextFile(path2) // 从hdfs获取数据源
    val csv: DataSet[(String, Int, String)] = env.readCsvFile[(String, Int, String)](csvPath, ignoreFirstLine = true) // 从csv文件获取数据源,指定类型，否则为Nothing，ignoreFirstLine忽略第一行。
    val csv2: DataSet[(String, Int)] = env.readCsvFile[(String, Int)](csvPath, ignoreFirstLine = true, includedFields = Array(0, 1)) // 从Csv文件获取数据源，指定类型，并且对需包含的字段索引进行指定。
    val csv3: DataSet[Person] = env.readCsvFile[Person](csvPath, ignoreFirstLine = true, pojoFields = Array("name", "age", "job"), includedFields = Array(0, 1, 2)) // 通过POJO对象【Java】进行csv的数据源读取
    val stringValue: DataSet[StringValue] = env.readTextFileWithValue(path, "UTF-8") // 从文件获取数据源，并指定编码，返回的StringValue可变
//        val sequence = env.readSequenceFile(classOf[IntWritable], classOf[Text],"hdfs://nnHost:nnPort/path/to/file") // 读取File文件
    // 从递归文件夹读取文件并创建数据源
    val parameter = new Configuration()
    parameter.setBoolean("recursive.file.enumeration", true) // 读取递归文件夹中文件内容
    val line3: DataSet[String] = env.readTextFile(parh0).withParameters(parameter)
    // 读取压缩文件并创建数据源
    val compression = env.readTextFile("压缩文件path") // 极其重要，压缩文件的读取极其重要！！！

    // 2、从集合创建数据源
    val seqSource: DataSet[Int] = env.fromCollection(seq) // 从序列Seq创建数据源,需相同类型
    val iteSource: DataSet[Int] = env.fromCollection(ite) // 从迭代器创建数据源
    val ele: DataSet[Int] = env.fromElements(1, 2, 3, 4, 5) // 从给定的对象序列创建数据源,需相同类型
    //    env.fromParallelCollection(// SplittableIterator) // 从迭代器并行创建数据源
    val gene: DataSet[Long] = env.generateSequence(1, 100000000) // 从给定间隔内并行创建数字序列数据源

    // 3、通用创建数据源
    //    val read = env.readFile() // 接收文件输入格式
    //    env.createInput() // 接收通用输入格式


  }
}
