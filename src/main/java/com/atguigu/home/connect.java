package com.atguigu.home;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Properties;

public class connect {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Integer> source_1 = env.fromElements(1, 2, 3, 4, 5, 6);
        DataStreamSource<String> source_2 = env.fromElements("a", "b", "c");
        ConnectedStreams<Integer, String> connect = source_1.connect(source_2);
        connect.map(new CoMapFunction<Integer, String, Object>() {
            @Override
            public Object map1(Integer value) throws Exception {
                return value*value;
            }

            @Override
            public Object map2(String value) throws Exception {
                return value+"aaaaa";
            }
        }).print();
        env.execute();
    }

    public  static class  MyFilter implements FilterFunction<String>{
            private  String s;

        public MyFilter(String s) {
            this.s = s;
        }

        @Override
        public boolean filter(String value) throws Exception {
            return value.equals(this.s);
        }
    }
}
