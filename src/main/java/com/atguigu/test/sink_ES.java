package com.atguigu.test;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.http.HttpHost;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class sink_ES {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> source = env.socketTextStream("hadoop102",9998);

        Properties properties = new Properties();
//        properties.setProperty("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092");
//        properties.setProperty("group.id", "Flink01_Source_Kafka");
//        properties.setProperty("auto.offset.reset", "latest");

//        ArrayList<HttpHost> httpHosts = new ArrayList<>();
//        httpHosts.add(new HttpHost("hadoop102",9200));
//        new ElasticsearchSink.Builder<String>(httpHosts, new ElasticsearchSinkFunction<String>() {
//            @Override
//            public void process(String s, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
//                Map<String, String> hashMap = new HashMap<>();
//            }
//        });


//        source.addSink();

        env.execute();
    }

}
