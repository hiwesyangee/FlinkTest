package com.hiwes.flink.datastream;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class SinkToMySQL extends RichSinkFunction<DataStreamSinkTest.Student> {

    Connection connection;
    PreparedStatement pstmt;

    private Connection getConnection() {
        Connection conn = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            String url = "jdbc:mysql://localhost:3306/test";
            conn = DriverManager.getConnection(url, "root", "root");
        } catch (Exception e) {
            e.printStackTrace();
        }
        return conn;
    }

    /**
     * 在open方法中建立connection
     *
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        connection = getConnection();
        String sql = "insert into Student(id,name,age) values (?,?,?)";
        pstmt = connection.prepareStatement(sql);
        System.out.println("open");
    }

    // 每条记录插入时调用一次
    public void invoke(DataStreamSinkTest.Student value, Context context) throws Exception {
        System.out.println("invoke~~~~~~~~~");
        // 未前面的占位符赋值
        pstmt.setInt(1, value.id());
        pstmt.setString(2, value.name());
        pstmt.setInt(3, value.age());
        pstmt.executeUpdate();
    }

    /**
     * 在close方法中要释放资源
     *
     * @throws Exception
     */
    @Override
    public void close() throws Exception {
        super.close();
        if (pstmt != null) {
            pstmt.close();
        }
        if (connection != null) {
            connection.close();
        }
    }
}
