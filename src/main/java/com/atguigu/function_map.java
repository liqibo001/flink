package com.atguigu;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class function_map {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = env.socketTextStream("hadoop102", 9998);
        source.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(",");
                return new WaterSensor(split[0],Long.valueOf(split[1]),Integer.valueOf(split[2]));
//                return null;
            }
        }).print();
        env.execute();
    }

    public static  class MyMapRichFunction extends RichMapFunction<String,WaterSensor>{
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
        }

        @Override
        public void close() throws Exception {
            super.close();
        }

        @Override
        public WaterSensor map(String value) throws Exception {
            return null;
        }
    }
}
