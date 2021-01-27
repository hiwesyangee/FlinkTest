package com.hiwes.flink.datastream

import java.sql.{Connection, DriverManager, PreparedStatement}

import com.hiwes.flink.datastream.DataStreamSinkTest.Student
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}

object ScalaSinkToMySQL extends RichSinkFunction[Student] {

  var connection: Connection = _
  var pstmt: PreparedStatement = _

  private def getConnection(): Connection = {
    var conn: Connection = null
    try {
      Class.forName("com.mysql.jdbc.Driver")
      val url = "jdbc:mysql://localhost:3306/test"
      conn = DriverManager.getConnection(url, "root", "root")
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
    conn
  }

  /**
   * 在open方法中建立connection
   *
   * @param parameters
   * @throws Exception
   */
  @throws[Exception]
  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    connection = getConnection
    val sql = "insert into Student(id,name,age) values (?,?,?)"
    pstmt = connection.prepareStatement(sql)
    System.out.println("open")
  }

  // 每条记录插入时调用一次
  @throws[Exception]
  override def invoke(value: Student, context: SinkFunction.Context[_]): Unit = {
    System.out.println("invoke~~~~~~~~~")
    // 未前面的占位符赋值
    pstmt.setInt(1, value.id)
    pstmt.setString(2, value.name)
    pstmt.setInt(3, value.age)
    pstmt.executeUpdate
  }

  /**
   * 在close方法中要释放资源
   *
   * @throws Exception
   */
  @throws[Exception]
  override def close(): Unit = {
    super.close()
    if (pstmt != null) pstmt.close()
    if (connection != null) connection.close()
  }


}
