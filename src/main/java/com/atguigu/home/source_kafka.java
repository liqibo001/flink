package com.atguigu.home;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Properties;

public class source_kafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","hadoop102:9092,hadoop103:9092,hadoop103:9092");
        properties.setProperty("group.id","flink_kafka");
        properties.setProperty("auto.offset.reset", "latest");
        FlinkKafkaConsumer<String> flink_kafka = new FlinkKafkaConsumer<>("flink_kafka", new SimpleStringSchema(), properties);
        env.addSource(flink_kafka).flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] strings = value.split(" ");
                for (String string : strings) {
                    out.collect(string);
                }
            }
        }).map(new MapFunction<String, Tuple2<String,Long>>() {
            @Override
            public Tuple2<String, Long> map(String value) throws Exception {
                return Tuple2.of(value,1L);
            }
        }).keyBy(new KeySelector<Tuple2<String, Long>, String>() {
            @Override
            public String getKey(Tuple2<String, Long> value) throws Exception {
                return value.f0;
            }
        }).sum(1).print();
        env.execute();
    }
}
