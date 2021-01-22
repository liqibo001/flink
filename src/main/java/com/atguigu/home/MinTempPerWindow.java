package com.atguigu.home;

import com.atguigu.test.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MinTempPerWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.socketTextStream("hadoop102",9999)
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] s = value.split(" ");
                        return new WaterSensor(s[0],Long.valueOf(s[1]),Integer.valueOf(s[2]));
                    }
                })
                .keyBy(WaterSensor::getId);
        env.execute();
    }
}
