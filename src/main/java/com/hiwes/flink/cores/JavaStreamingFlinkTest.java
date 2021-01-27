package com.hiwes.flink.cores;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * 使用Java处理实时数据。
 *
 * WordCount统计的数据，源自Socket。
 */
public class JavaStreamingFlinkTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> text = env.socketTextStream("hiwes", 9999);

        text.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] arr = s.split(",");
                for (String a : arr) {
                    collector.collect(new Tuple2<>(a, 1));
                }
            }
        }).keyBy(0).timeWindow(Time.seconds(5)).sum(1).print();


        env.execute("JavaStreamingWordCount");
    }
}
