package com.atguigu.test;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class wordCount_Batch {
    public static void main(String[] args) throws Exception {
        //1.初始化ssc，执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //2.读取数据
        DataSource<String> inputDS = env.readTextFile("input/words.txt");
        //3.处理数据
        //3.1 flatmap：切分成word
        FlatMapOperator<String, String> wordDS = inputDS.flatMap((FlatMapFunction<String, String>) (s, collector) -> {
            String[] words = s.split(" ");
            for (String word : words) {
                collector.collect(word);
            }
        }).returns(Types.STRING);
        //3.2 转换成元组（word，1）
        MapOperator<String, Tuple2<String, Long>> wordAndOneDS = wordDS.map( s -> Tuple2.of(s, 1L)).returns(Types.TUPLE(Types.STRING,Types.LONG));
        //3.3  按照word 分组
        UnsortedGrouping<Tuple2<String, Long>> wordAndOneGroup = wordAndOneDS.groupBy(0);
        //3.4 组内聚合
        AggregateOperator<Tuple2<String, Long>> resultDS = wordAndOneGroup.sum(1);
        //4.输出
        resultDS.print();
        //5.执行(不需要)



    }
}

/*如果使用lambda 表达式 可能因为类型擦除 引起报错  可以在后面使用return指定类型
* */