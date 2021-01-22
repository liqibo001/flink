package com.atguigu.chapter06;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashSet;

public class Project_UV {
    public static void main(String[] args) throws Exception {
        //创建执行环境
        StreamExecutionEnvironment env= StreamExecutionEnvironment.getExecutionEnvironment();
        //读取数据
        DataStreamSource<String> inputDS = env.readTextFile("input/UserBehavior.csv");
      inputDS.flatMap(new FlatMapFunction<String, String>() {
          @Override
          public void flatMap(String value, Collector<String> out) throws Exception {
              String[] s = value.split(",");
              String s1 = s[3];
              if ("pv".equals(s1)){
                  out.collect(s[0]);
              }
          }
      })
        .map(new MapFunction<String, Tuple2<String,String>>() {
            @Override
            public Tuple2<String, String> map(String value) throws Exception {
                return Tuple2.of("uv",value);
            }
        })
              .keyBy(r ->r.f0)
              .process(new KeyedProcessFunction<String, Tuple2<String, String>, Long>() {
                  HashSet<String> ucCount = new HashSet<>();
                  @Override
                  public void processElement(Tuple2<String, String> value, Context ctx, Collector<Long> out) throws Exception {
                      ucCount.add(value.f1);
                      out.collect((long) ucCount.size());
                  }
              }).print();
        env.execute();
    }
}
