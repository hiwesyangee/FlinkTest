package com.hiwes.flink.Zinterview.highlevel_4

import java.sql.{Connection, DriverManager, SQLException}

object DBConnectUtils {}

class DBConnectUtils {
  // 获取连接
  def getConnection(url: String, user: String, password: String): Connection = {
    var conn: Connection = null

    try {
      Class.forName("com.mysql.jdbc.Driver")
    } catch {
      case e: ClassNotFoundException => e.printStackTrace()
    }

    conn = DriverManager.getConnection(url, user, password)

    // 设置手动提交
    conn.setAutoCommit(false)
    conn
  }

  // 提交事务
  def commit(conn: Connection): Unit = {
    if (conn != null) {
      try {
        conn.commit()
      } catch {
        case e: SQLException => e.printStackTrace()
      } finally {
        close(conn)
      }
    }
  }

  // 事务回滚
  def rollback(conn: Connection): Unit = {
    if (conn != null) {
      try {
        conn.rollback()
      } catch {
        case e: SQLException => e.printStackTrace()
      } finally {
        close(conn)
      }
    }
  }

  // 关闭连接
  def close(conn: Connection): Unit = {
    if (conn != null) {
      try {
        conn.close()
      } catch {
        case e: SQLException => e.printStackTrace()
      }
    }
  }

}
