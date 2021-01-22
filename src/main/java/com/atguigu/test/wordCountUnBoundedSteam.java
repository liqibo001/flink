package com.atguigu.test;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class wordCountUnBoundedSteam {
    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
//        env.disableOperatorChaining();  //全局禁用操作链
           env.socketTextStream("hadoop102", 9999).flatMap(new FlatMapFunction<String, String>() {
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
       })
//                   .disableChaining()  //前后都禁用 链条
//                   .startNewChain()//以当前算子 重新开链条
                .keyBy(0).sum(1).print();
        env.execute();
    }
}
