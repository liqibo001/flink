package com.atguigu.chapter06;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class Project_PV_2 {
    public static void main(String[] args) throws Exception {
        //创建执行环境
        StreamExecutionEnvironment env= StreamExecutionEnvironment.getExecutionEnvironment();
        //读取数据
        DataStreamSource<String> inputDS = env.readTextFile("input/UserBehavior.csv");
      inputDS.flatMap(new FlatMapFunction<String, Tuple2<String,Long>>() {
          @Override
          public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
              String[] s = value.split(",");
              String s1 = s[3];
              if ("pv".equals(s1)){
                  out.collect(Tuple2.of(s1,1L));
              }
          }
      })
        .keyBy(r->r.f0)
              .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                  @Override
                  public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                      return Tuple2.of(value1.f0,value1.f1+value2.f1);
                  }
              }).print();
        env.execute();
    }
}
