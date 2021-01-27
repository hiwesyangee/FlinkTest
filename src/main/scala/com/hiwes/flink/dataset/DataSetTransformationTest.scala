package com.hiwes.flink.dataset

import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala._

import scala.collection.mutable.ListBuffer

/**
 * DataSet API的Transformation测试。
 */
object DataSetTransformationTest {

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    //    mapFunction(env)
    //    flatMapFunction(env)
    //    mapPartitionFunction(env)
    //    filterFunction(env)
    //    firstFunction(env)
    //    distinctFunction(env)
    //    joinFunction(env)
    //    leftJoinFunction(env)
    //    rightJoinFunction(env)
    //    fullOuterJoinFunction(env)
    crossFunction(env)
  }

  // map
  def mapFunction(env: ExecutionEnvironment): Unit = {
    val score: DataSet[Int] = env.fromCollection(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
    score.map(_ + 1).print()
  }

  // flatMap
  def flatMapFunction(env: ExecutionEnvironment): Unit = {
    val score: DataSet[String] = env.readTextFile("file:///Users/hiwes/data/test1.txt")
    val result: DataSet[String] = score.flatMap(_.split("\t"))
    result.print()
  }

  // mapPartition
  def mapPartitionFunction(env: ExecutionEnvironment): Unit = {
    val students = new ListBuffer[String]
    for (i <- 1 to 100) {
      students.append("student:" + i)
    }
    val data = env.fromCollection(students)

    data.mapPartition(ite => {
      val connection = "获取数据库链接"
      ite.map(x => {
        // 对x进行操作
      })
      //      println("此处释放连接")
    })
  }

  // filter
  def filterFunction(env: ExecutionEnvironment): Unit = {
    val score: DataSet[Int] = env.fromCollection(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
    score.filter(_ % 2 == 0).print()
  }

  // first
  def firstFunction(env: ExecutionEnvironment): Unit = {
    val info = ListBuffer[(Int, String)]()
    info.append((1, "hadoop"))
    info.append((1, "spark"))
    info.append((1, "flink"))
    info.append((2, "java"))
    info.append((2, "springboot"))
    info.append((3, "linux"))
    info.append((4, "vue"))

    //    env.fromCollection(info).first(2).print()
    //    env.fromCollection(info).groupBy(0).first(2).print() // 注意，这里是每一组取2条
    env.fromCollection(info).groupBy(0).sortGroup(1, Order.ASCENDING).first(2).print() // 根据组进行排序，然后再每一组取2条
  }

  // distinct
  def distinctFunction(env: ExecutionEnvironment): Unit = {
    val data: DataSet[Int] = env.fromElements(1, 2, 3, 2, 3)
    data.print()
    println("===")
    data.distinct().print()
  }

  // join
  def joinFunction(env: ExecutionEnvironment): Unit = {
    val students = ListBuffer[(Int, String)]()
    students.append((1, "tom"))
    students.append((2, "jerry"))
    students.append((3, "jackey"))
    val scores = ListBuffer[(Int, Int)]()
    scores.append((1, 100))
    scores.append((2, 98))
    scores.append((4, 90))

    val input1 = env.fromCollection(students)
    val input2 = env.fromCollection(scores)

    input1.join(input2).where(0).equalTo(0).apply((first, second) => {
      (first._1, first._2, second._2)
    }).print()
  }

  // leftJoin   包含左边所有的元素，但是要注意，需要对为空的数据进行判断。
  def leftJoinFunction(env: ExecutionEnvironment): Unit = {
    val students = ListBuffer[(Int, String)]()
    students.append((1, "tom"))
    students.append((2, "jerry"))
    students.append((3, "jackey"))
    val scores = ListBuffer[(Int, Int)]()
    scores.append((1, 100))
    scores.append((2, 98))
    scores.append((4, 90))

    val input1 = env.fromCollection(students)
    val input2 = env.fromCollection(scores)
    input1.leftOuterJoin(input2).where(0).equalTo(0).apply((first, second) => {
      if (null == second) (first._1, first._2, "---")
      else (first._1, first._2, second._2)
    }).print()
  }

  // rightJoin
  def rightJoinFunction(env: ExecutionEnvironment): Unit = {
    val students = ListBuffer[(Int, String)]()
    students.append((1, "tom"))
    students.append((2, "jerry"))
    students.append((3, "jackey"))
    val scores = ListBuffer[(Int, Int)]()
    scores.append((1, 100))
    scores.append((2, 98))
    scores.append((4, 90))

    val input1 = env.fromCollection(students)
    val input2 = env.fromCollection(scores)

    input1.rightOuterJoin(input2).where(0).equalTo(0).apply((first, second) => {
      if (null == first) (second._1, "---", second._2)
      else (first._1, first._2, second._2)
    }).print()
  }

  // fullOuterJoin
  def fullOuterJoinFunction(env: ExecutionEnvironment): Unit = {
    val students = ListBuffer[(Int, String)]()
    students.append((1, "tom"))
    students.append((2, "jerry"))
    students.append((3, "jackey"))
    val scores = ListBuffer[(Int, Int)]()
    scores.append((1, 100))
    scores.append((2, 98))
    scores.append((4, 90))

    val input1 = env.fromCollection(students)
    val input2 = env.fromCollection(scores)

    input1.fullOuterJoin(input2).where(0).equalTo(0).apply((first, second) => {
      if (null == first) (second._1, "---", second._2)
      else if (null == second) (first._1, first._2, "---")
      else (first._1, first._2, second._2)
    }).print()
  }

  // cross: 组合每个元素。
  def crossFunction(env: ExecutionEnvironment): Unit = {
    val data1 = List("people1", "people2", "people3")
    val data2 = List(1, 2, 3, 4, 5)
    val input1 = env.fromCollection(data1)
    val input2 = env.fromCollection(data2)
    input1.cross(input2).print()
  }
}
