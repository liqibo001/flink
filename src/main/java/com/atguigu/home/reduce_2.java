package com.atguigu.home;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class reduce_2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source1 = env.socketTextStream("hadoop102", 9999);
        DataStreamSource<String> source2 = env.socketTextStream("hadoop102", 9998);
        DataStream<String> dataStream = source1.union(source2);
        dataStream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] s = value.split(" ");
                for (String s1 : s) {
                    out.collect(s1);
                }
            }
        })
                .filter(new FilterFunction<String>() {
                    @Override
                    public boolean filter(String value) throws Exception {
                        if (value.contains("a")) {
                            return false;
                        } else {
                            return true;
                        }
                    }
                })
                .map(new MapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(String value) throws Exception {
                        return Tuple2.of(value, 1L);
                    }
                })
                .keyBy(new KeySelector<Tuple2<String, Long>, Object>() {
                    @Override
                    public Object getKey(Tuple2<String, Long> value) throws Exception {
                        return value.f0;
                    }
                })
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                        return Tuple2.of(value1.f0,value1.f1+value2.f1);
                    }
                })
                .print();
        env.execute();
    }
}
