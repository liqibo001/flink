package com.atguigu.chapter06;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class AppAnalysis_By_Chanel {
    public static void main(String[] args) throws Exception {
        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.addSource(new MySource())
                .map(new MapFunction<MarketingUserBehavior, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(MarketingUserBehavior value) throws Exception {
                        return Tuple2.of(value.getBehavior() + "_" + value.getChannel(), 1L);
                    }
                }).keyBy(0).sum(1).print();

        env.execute();
    }

    public static class MySource implements SourceFunction<MarketingUserBehavior> {

        private volatile boolean isRunning = true;
        private Random random = new Random();
        private List<String> behavior = Arrays.asList("DOWNLOAD", "INSTALL", "UNINSTALL", "UPDATE");
        private List<String> channel = Arrays.asList("xiaomi", "HUAWEI", "OPPO", "VIVO", "APPSTORE");

        @Override
        public void run(SourceContext<MarketingUserBehavior> ctx) throws Exception {

            while (isRunning) {
                MarketingUserBehavior marketingUserBehavior = new MarketingUserBehavior(
                        random.nextLong(),
                        behavior.get(random.nextInt(behavior.size())),
                        channel.get(random.nextInt(channel.size())),
                        System.currentTimeMillis()
                );

                ctx.collect(marketingUserBehavior);
                Thread.sleep(1000);
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }
}
