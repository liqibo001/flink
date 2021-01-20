package com.atguigu;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

public class function_connect {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Integer> source1 = env.fromElements(1, 2, 3, 4, 5, 6);
        DataStreamSource<Integer> source2 = env.fromElements(7,8,9,10);
        ConnectedStreams<Integer, Integer> connect = source1.connect(source2);
        connect.map(new CoMapFunction<Integer, Integer, Object>() {
            @Override
            public Object map1(Integer value) throws Exception {
                return value;
            }

            @Override
            public Object map2(Integer value) throws Exception {
                return value+100;
            }
        }).print();
        env.execute();
    }
}
