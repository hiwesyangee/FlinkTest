package com.hiwes.flink.datastream

import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}

class DataStreamCustomRichParallelSourceFunction extends RichParallelSourceFunction[Long] {
  var isRunning = true
  var count = 1l

  override def run(ctx: SourceFunction.SourceContext[Long]): Unit = {
    while (isRunning) {
      ctx.collect(count) // 通过collect发送出去,可以自定义count的格式。
      count += 1
      Thread.sleep(1000)
    }
  }

  override def cancel(): Unit = {
    isRunning = false
  }
}
