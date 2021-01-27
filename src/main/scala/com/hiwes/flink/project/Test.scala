package com.hiwes.flink.project

import scala.collection.mutable

object Test {

  def main(args: Array[String]): Unit = {
    val map = mutable.HashMap[String, String]()
    map.update("1", "test")
    map.update("2", "test2")
    map.+=(("3", "test3"))

    println(map.getOrElse("4","1"))
    println(map.getOrElse("3","1"))

    map.getOrElseUpdate("4",op = "test4")
    println(map.getOrElse("4","1"))
    map.update("4","tttt")
    println(map.getOrElse("4","1"))

    map.-=("3","test3")
    map.remove("1")
    println(map.getOrElse("3","none"))
    println(map.getOrElse("1","None"))

  }

}
