package com.atguigu.test;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class wordCount_BoundedStream {
    public static void main(String[] args) throws Exception {
        //创建执行环境
        StreamExecutionEnvironment env= StreamExecutionEnvironment.getExecutionEnvironment();
        //读取数据
        DataStreamSource<String> inputDS = env.readTextFile("input/words.txt");
        //处理数据
        inputDS.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                String[] words = s.split(" ");
                for (String word : words) {
                    collector.collect(word);
                }
            }
        }).map(new MapFunction<String, Tuple2<String,Long>>() {
            @Override
            public Tuple2<String, Long> map(String s) throws Exception {
                return Tuple2.of(s,1L);
            }
        }).keyBy(0).sum(1).print();

        //输出
        env.execute();
    }
}
