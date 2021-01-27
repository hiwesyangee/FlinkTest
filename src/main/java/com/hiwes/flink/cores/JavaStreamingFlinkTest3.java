package com.hiwes.flink.cores;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * 使用Java处理实时数据3。
 * <p>
 * WordCount统计的数据，源自Socket。
 * 增加参数的外部获取方式。
 * 使用字段表达式定义K。
 */
public class JavaStreamingFlinkTest3 {

    public static void main(String[] args) throws Exception {
        String host = "";
        int port = 0;

        try {
            ParameterTool tool = ParameterTool.fromArgs(args);
            host = tool.get("host");
            port = tool.getInt("port");
        } catch (Exception e) {
            System.err.println("端口未设置,使用默认host: localhost,和默认端口: 9999.");
            host = "localhost";
            port = 9999;
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> text = env.socketTextStream(host, port);

        text.flatMap(new FlatMapFunction<String, WC>() {
            @Override
            public void flatMap(String s, Collector<WC> collector) throws Exception {
                String[] arr = s.split(",");
                for (String a : arr) {
                    collector.collect(new WC(a, 1));
                }
            }
        })
                .keyBy("word")
                .timeWindow(Time.seconds(5))
                .sum("count")
                .print();
//        text.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
//            @Override
//            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
//                String[] arr = s.split(",");
//                for (String a : arr) {
//                    collector.collect(new Tuple2<>(a, 1));
//                }
//            }
//        }).keyBy(0).timeWindow(Time.seconds(5)).sum(1).print();


        env.execute("JavaStreamingWordCount");
    }

    public static class WC {
        private String word;
        private int count;


        public WC() {
        }

        public WC(String word, int count) {
            this.word = word;
            this.count = count;
        }

        public String getWord() {
            return word;
        }

        public void setWord(String word) {
            this.word = word;
        }

        public int getCount() {
            return count;
        }

        public void setCount(int count) {
            this.count = count;
        }

        public String toString() {
            return "WC{" +
                    "word='" + word + '\'' +
                    ", count=" + count +
                    '}';
        }
    }
}
