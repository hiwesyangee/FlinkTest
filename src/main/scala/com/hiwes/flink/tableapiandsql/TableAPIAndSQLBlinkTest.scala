package com.hiwes.flink.tableapiandsql

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.scala.{BatchTableEnvironment, StreamTableEnvironment}
import org.apache.flink.table.api.{EnvironmentSettings, Table, TableEnvironment}

/**
 * 1.10.2版本TableAPI & SQL.
 * 使用了Blink Batch Query.
 */
object TableAPIAndSQLBlinkTest {
  def main(args: Array[String]): Unit = {
    val csvPath = "file:///Users/hiwes/data/flink/sales.csv"

    val bsEnv = StreamExecutionEnvironment.getExecutionEnvironment
    val bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(bsEnv, bsSettings)

    //    val ds: DataStream[String] = env.readTextFile("file:///Users/hiwes/data/flink/sales.txt")
    //    val ds2: DataStream[Tuple4[String, String, String, Double]] = ds.map(x => {
    //      val arr = x.split(",")
    //      Tuple4(arr(0), arr(1), arr(2), arr(3).toDouble)
    //    })

    val ds: DataStream[salesLog] = env.socketTextStream("hiwes", 9999).map(x => {
      val arr = x.split(",")
      salesLog(arr(0), arr(1), arr(2), arr(3).toDouble)
    })

    val table: Table = tableEnv.fromDataStream(ds)
    val ds3: DataStream[String] = tableEnv.toAppendStream(table)
    ds3.print()


    //    注册为一个表
//    tableEnv.createTemporaryView("table", table)
    //
    //    val table02 = tableEnv.sqlQuery("select * from table").select("name")
    //
    //    将表转换DataStream//将表转换DataStream
    //    val ds3: DataStream[String] = tableEnv.toAppendStream(table02)
    //    ds3.print()

//    tableEnv.execute("Flink stream sql")
  }

  case class salesLog(transactionId: String, customerId: String, itemId: String, amountPaid: Double)

}
