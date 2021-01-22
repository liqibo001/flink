package com.atguigu.test;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

public class Source_Custom {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.addSource(new MySource()).print();
        env.execute();

    }

    public static class MySource implements SourceFunction<WaterSensor> {
        private volatile Boolean flag = true;
        //volatile 在多线程的情况下 保证可见性  保证原子的可变性
        @Override
        public void run(SourceContext<WaterSensor> ctx) throws Exception {

            Random random = new Random();
            while (flag){
                ctx.collect(
                        new WaterSensor("ws_"+random.nextInt(100), System.currentTimeMillis(), random.nextInt(200))
                );

            }
        }

        @Override
        public void cancel() {
            flag=false;
        }
    }
}
