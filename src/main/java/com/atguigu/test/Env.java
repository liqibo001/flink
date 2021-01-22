package com.atguigu.test;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Env {
    public static void main(String[] args) {
        //批执行环境
        ExecutionEnvironment bend = ExecutionEnvironment.getExecutionEnvironment();
        //流执行环境
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        //流执行环境下 指定为批处理模式
        senv.setRuntimeMode(RuntimeExecutionMode.BATCH);

        //spark里面运行环境需要明确指定
            //idea 运行时候  创建ssc.setMaster(local[*])
            // 如果要提交到集群去执行 不写setMaster(local[*])
    }
}
