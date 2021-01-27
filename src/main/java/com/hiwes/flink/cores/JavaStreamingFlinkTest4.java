package com.hiwes.flink.cores;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * 使用Java处理实时数据4。
 * <p>
 * WordCount统计的数据，源自Socket。
 * 创建自己的FlatMapFunction，在调用时直接指定即可。
 */
public class JavaStreamingFlinkTest4 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> text = env.socketTextStream("hiwes", 9999);

        text.flatMap(new MyFlatMapFunction()).keyBy(0).timeWindow(Time.seconds(5)).sum(1).print();

//        text.map(new MapFunction<String, Integer>() {
//            // 也可以将函数作为匿名类传递。
//            public Integer map(String value) {return Integer.parseInt(value);}
//        });

        env.execute("JavaStreamingWordCount");
    }

    // 通过创建自定义类的方式实现FlatMapFunction。
    public static class MyFlatMapFunction implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
            String[] arr = s.split(",");
            for (String a : arr) {
                collector.collect(new Tuple2<>(a, 1));
            }
        }
    }
}
