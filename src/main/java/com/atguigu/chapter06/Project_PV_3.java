package com.atguigu.chapter06;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class Project_PV_3 {
    public static void main(String[] args) throws Exception {
        //创建执行环境
        StreamExecutionEnvironment env= StreamExecutionEnvironment.getExecutionEnvironment();
        //读取数据
        DataStreamSource<String> inputDS = env.readTextFile("D:\\program\\IDEA\\Git\\flink\\input\\UserBehavior.csv");
        inputDS.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                String[] s = value.split(",");
                String s1 = s[3];
                if ("pv".equals(s1)){
                    return true;
                }else {
                    return false;
                }

            }
        })
                .keyBy(new KeySelector<String, Tuple2<String,Long>>() {
                    @Override
                    public Tuple2<String, Long> getKey(String value) throws Exception {
                        String[] s = value.split(",");
                        String s1 = s[3];
                        return Tuple2.of(s1,1L);
                    }
                })

                .process(new KeyedProcessFunction<Tuple2<String, Long>, String, Object>() {
                    @Override
                    public void processElement(String value, Context ctx, Collector<Object> out) throws Exception {

                    }
                })
                .print();
        env.execute();
    }
}
