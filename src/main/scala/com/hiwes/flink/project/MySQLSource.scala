package com.hiwes.flink.project

import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}

import scala.collection.mutable

class MySQLSource extends RichParallelSourceFunction[mutable.HashMap[String,String]]{
  override def run(ctx: SourceFunction.SourceContext[mutable.HashMap[String, String]]): Unit = ???

  override def cancel(): Unit = ???
}
