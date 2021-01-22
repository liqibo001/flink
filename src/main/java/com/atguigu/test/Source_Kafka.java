package com.atguigu.test;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.ArrayList;
import java.util.Properties;

public class Source_Kafka {
    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092");
        properties.setProperty("group.id", "Flink01_Source_Kafka");
        properties.setProperty("auto.offset.reset", "latest");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ArrayList<String> list = new ArrayList<>();
        list.add("flink_kafka");
        env.addSource(new FlinkKafkaConsumer<String>(list, new SimpleStringSchema(), properties)).print();
        env.execute();

    }
}
