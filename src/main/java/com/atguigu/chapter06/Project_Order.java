package com.atguigu.chapter06;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

public class Project_Order {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        SingleOutputStreamOperator<OrderEvent> orderDS = env
                .readTextFile("input/OrderLog.csv")
                .map(new MapFunction<String, OrderEvent>() {
                    @Override
                    public OrderEvent map(String value) throws Exception {
                        String[] datas = value.split(",");
                        return new OrderEvent(Long.valueOf(datas[0]), datas[1], datas[2], Long.valueOf(datas[3]));
                    }
                });
        SingleOutputStreamOperator<TxEvent> txDS = env
                .readTextFile("input/ReceiptLog.csv")
                .map(new MapFunction<String, TxEvent>() {
                    @Override
                    public TxEvent map(String value) throws Exception {
                        String[] strings = value.split(",");
                        String txid = strings[0];
                        String paychannel = strings[1];
                        Long eventTime = Long.valueOf(strings[2]);
                        return new TxEvent(txid, paychannel, eventTime);
                    }
                });

        ConnectedStreams<OrderEvent, TxEvent> orderTxCS = orderDS.connect(txDS);

        orderTxCS.process(new OrderTxDetectFunction())
                .print();


        env.execute();
    }

    public static class OrderTxDetectFunction extends CoProcessFunction<OrderEvent, TxEvent, String> {

        Map<String, TxEvent> txEventMap = new HashMap<>();

        Map<String, OrderEvent> orderEventMap = new HashMap<>();

        @Override
        public void processElement1(OrderEvent value, Context ctx, Collector<String> out) throws Exception {
                if (txEventMap.containsKey(value.getTxId())){
                    out.collect("订单"+value.getOrderId()+"对账成功");
                    txEventMap.remove(value.getTxId());
                }else {
                    orderEventMap.put(value.getTxId(),value);
                }
        }

        @Override
        public void processElement2(TxEvent value, Context ctx, Collector<String> out) throws Exception {
            if (orderEventMap.containsKey(value.getTxId())){
                out.collect("订单" + orderEventMap.get(value.getTxId()).getOrderId() + "对账成功！");
                orderEventMap.remove(value.getTxId());
            }else {
                txEventMap.put(value.getTxId(), value);
            }
        }
    }
}
