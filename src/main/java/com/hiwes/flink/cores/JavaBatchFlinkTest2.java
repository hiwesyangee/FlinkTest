package com.hiwes.flink.cores;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * 使用Java批处理离线数据。
 */
public class JavaBatchFlinkTest2 {

    public static void main(String[] args) throws Exception {
        String path = "file:///Users/hiwes/data/test1.txt";

        // 1.获取运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 2.读取数据
        DataSource<String> text = env.readTextFile(path);

        // 3.按指定分隔符拆分每一行数据，并赋次数1，之后进行合并。
        text.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] split = s.toLowerCase().split("\t");
                for (String sp : split) {
                    if (sp.length() > 0) {
                        collector.collect(new Tuple2<>(sp, 1));
                    }
                }
            }
        }).groupBy(0).sum(1).print(); // groupBy(0)，可以传递一个指定position位置的值，0就是key。


    }
}
