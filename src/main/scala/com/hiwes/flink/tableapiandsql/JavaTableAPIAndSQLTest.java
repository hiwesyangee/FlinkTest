package com.hiwes.flink.tableapiandsql;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;

public class JavaTableAPIAndSQLTest {

    public static void main(String[] args) {

        String csvPath = "file:///Users/hiwes/data/flink/sales.csv";

        ExecutionEnvironment fbEnv = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment fbTableEnv = BatchTableEnvironment.create(fbEnv);


    }
}
