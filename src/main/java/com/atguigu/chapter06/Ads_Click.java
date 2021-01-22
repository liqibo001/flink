package com.atguigu.chapter06;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Ads_Click {
    public static void main(String[] args) throws Exception {
        //创建执行环境
        StreamExecutionEnvironment env= StreamExecutionEnvironment.getExecutionEnvironment();
        //读取数据
        DataStreamSource<String> inputDS = env.readTextFile("input/AdClickLog.csv");
      inputDS.map(new MapFunction<String, Tuple2<String,Long>>() {
          @Override
          public Tuple2<String, Long> map(String value) throws Exception {
              String[] split = value.split(",");
              String provence = split[2];
              String adid = split[1];
              return Tuple2.of(provence+"_"+adid,1L);
          }
      }).keyBy(r ->r.f0).sum(1).print();
        env.execute();
    }
}
