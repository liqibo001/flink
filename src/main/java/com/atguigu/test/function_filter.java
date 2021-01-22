package com.atguigu.test;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class function_filter {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> source = env.socketTextStream("hadoop102", 9998);
       source.filter(new FilterFunction<String>() {
           @Override
           public boolean filter(String value) throws Exception {
               if (value.contains("a")){
                   return false;
               }else {
                   return true;
               }
           }
       }).print();
        env.execute();
    }
}
