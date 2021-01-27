package com.hiwes.flink.tableapiandsql

import org.apache.flink.api.scala._
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala.BatchTableEnvironment
import org.apache.flink.types.Row

/**
 * 1.10.2版本TableAPI & SQL.
 * 使用了Flink Batch Query.
 */
object TableAPIAndSQLFlinkTest {

  def main(args: Array[String]): Unit = {

    val csvPath = "file:///Users/hiwes/data/flink/sales.csv"

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = BatchTableEnvironment.create(env)

    val csv = env.readCsvFile[salesLog](csvPath, ignoreFirstLine = true)
    val salesTable: Table = tableEnv.fromDataSet(csv)

    //    tableEnv.registerTable("sales", salesTable) // 1.7版本写法
    tableEnv.createTemporaryView("sales", salesTable)

    val result: Table = tableEnv.sqlQuery("select customerId, sum(amountPaid) money from sales group by customerId")

    tableEnv.toDataSet[Row](result).print()

    result.printSchema()


    env.execute("TableAPIAndSQLTest")

  }

  case class salesLog(transactionId: String, customerId: String, itemId: String, amountPaid: Double)

}
