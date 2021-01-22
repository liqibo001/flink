package com.atguigu.test;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisDataType;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

import java.util.Properties;

public class sink_redis {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> source = env.socketTextStream("hadoop102",9998);

        Properties properties = new Properties();
//        properties.setProperty("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092");
//        properties.setProperty("group.id", "Flink01_Source_Kafka");
//        properties.setProperty("auto.offset.reset", "latest");

        FlinkJedisPoolConfig config = new FlinkJedisPoolConfig.Builder()
                .setHost("hadoop102")
                .setPort(6379)
                .setMaxTotal(100)
                .setTimeout(1000 * 10)
                .build();

        RedisSink<String> flink_redis = new RedisSink<>(config, new RedisMapper<String>() {
            @Override
            public RedisCommandDescription getCommandDescription() {
                return new RedisCommandDescription(RedisCommand.HSET, "flink_redis");
            }

            @Override
            public String getKeyFromData(String s) {
                return s;
            }

            @Override
            public String getValueFromData(String s) {
                return String.valueOf(1);
            }
        });

        source.addSink(flink_redis);

        env.execute();
    }

}
