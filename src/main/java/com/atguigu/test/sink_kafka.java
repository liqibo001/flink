package com.atguigu.test;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;

import java.util.Properties;

public class sink_kafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> source = env.socketTextStream("hadoop102",9998);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092");
//        properties.setProperty("group.id", "Flink01_Source_Kafka");
//        properties.setProperty("auto.offset.reset", "latest");

        FlinkKafkaProducer<String> flink_kafka = new FlinkKafkaProducer<>("flink_kafka", new SimpleStringSchema(), properties);
        source.addSink(flink_kafka);

        env.execute();
    }

}
