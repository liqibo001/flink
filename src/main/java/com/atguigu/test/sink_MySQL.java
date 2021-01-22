package com.atguigu.test;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;


import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class sink_MySQL {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> source = env.socketTextStream("hadoop102",9998);





//        source.addSink();

        env.execute();
    }

    public  static class MySQLSinkFunction extends RichSinkFunction<String> {
        Connection connection;
        PreparedStatement preparedStatement;

        @Override
        public void invoke(String value, Context context) throws Exception {

        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
        }

        @Override
        public void close() throws Exception {
            preparedStatement.close();
        }
    }
}
