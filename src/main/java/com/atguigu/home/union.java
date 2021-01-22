package com.atguigu.home;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

public class union {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Integer> source_1 = env.fromElements(1, 2, 3, 4, 5, 6);
        DataStreamSource<Integer> source_2 = env.fromElements(7,8,9);
       source_1.union(source_2).map(new MapFunction<Integer, Object>() {
           @Override
           public Object map(Integer value) throws Exception {
               return value.toString();
           }
       }).print();
        env.execute();
    }
}
