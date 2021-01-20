package com.atguigu;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class function_keyby {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> source = env.socketTextStream("hadoop102", 9998);
       source.flatMap(new FlatMapFunction<String, String>() {
           @Override
           public void flatMap(String value, Collector<String> out) throws Exception {
               String[] split = value.split(" ");
               for (String s : split) {
                   out.collect(s);
               }
           }
       }).map(new MapFunction<String, Tuple2<String,Long>>() {
           @Override
           public Tuple2<String, Long> map(String value) throws Exception {
               return Tuple2.of(value,1L);
           }
       })
               .keyBy(new KeySelector<Tuple2<String, Long>, Object>() {
           @Override
           public Object getKey(Tuple2<String, Long> value) throws Exception {
               return value.f0 ;
           }
       })
               .sum(1).print();
        //Tuple => 具体的tuple  tuple1=> 取出 tuple1.f0
        env.execute();
    }
}
