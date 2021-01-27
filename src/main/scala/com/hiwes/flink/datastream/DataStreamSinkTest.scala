package com.hiwes.flink.datastream

import org.apache.flink.api.common.functions.{MapFunction, RichMapFunction}
import org.apache.flink.streaming.api.scala._

/**
 * DataStream API Sink测试————————实现自定义Sink.
 * 需求：通过socket发送数据，将string转为对象，然后将对象保存到mysql数据库中。
 *
 * 1】继承RichSinkFunction[T] T就是想写入对象的类型
 * 2】重写方法：open/close 生命周期方法;
 *            invoke  每条记录执行一次.
 */
object DataStreamSinkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val source = env.socketTextStream("hiwes", 9999)
    val data: DataStream[Student] = source.map(new MapFunction[String, Student] {
      override def map(in: String): Student = {
        val splits = in.split(",")
        Student(splits(0).toInt, splits(1), splits(2).toInt)
      }
    })

    data.addSink(ScalaSinkToMySQL)
//    data.addSink(new SinkToMySQL())

    env.execute("DataStreamSinkTest")
  }


  case class Student(id: Int, name: String, age: Int)

  //  create table Student(
  //    id int(11) NOT NULL AUTO_INCREMENT,
  //    name varchar(25),
  //    age int(10),
  //    primary key(id)
  //  )
}
