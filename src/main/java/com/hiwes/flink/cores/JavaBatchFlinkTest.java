package com.hiwes.flink.cores;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;

/**
 * 使用Java批处理离线数据。
 */
public class JavaBatchFlinkTest {
    public static void main(String[] args) throws Exception {
        String path = "file:///Users/hiwes/data/test1.txt";

        // 1.获取运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 2.读取数据
        DataSource<String> text = env.readTextFile(path);

        // 3.操作数据
        text.print();

    }
}
