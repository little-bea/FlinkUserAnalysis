package com.myflink.orderpay_detect;

import com.myflink.orderpay_detect.beans.OrderEvent;
import com.myflink.orderpay_detect.beans.OrderResult;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.net.URL;

/**
 * @author xuzengfeng
 * @description
 */
public class OrderTimeoutWithoutCep {

    // 定义超时事件的侧输出流标签
    private final static OutputTag<OrderResult> orderTimeoutTag = new OutputTag<OrderResult>("order-timeout"){};


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 读取数据并转换成POJO类型
        URL resource = OrderPayTimeout.class.getResource("/OrderLog.csv");
        DataStream<OrderEvent> orderEventStream = env.readTextFile(resource.getPath())
                .map(line -> {
                    String[] fields = line.split(",");
                    return new OrderEvent(new Long(fields[0]), fields[1], fields[2], new Long(fields[3]));
                })
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<OrderEvent>() {

                    @Override
                    public long extractAscendingTimestamp(OrderEvent orderEvent) {
                        return orderEvent.getTimestamp() * 1000L;
                    }
                });

        SingleOutputStreamOperator<OrderResult> resultStream = orderEventStream
                .keyBy(OrderEvent::getOrderId)
                .process(new OrderPayMatchDetect());

        resultStream.print("payed normally");
        resultStream.getSideOutput(orderTimeoutTag).print("tiemout");

        env.execute();
    }


    // 实现自定义KeyedProcessFunction
    public static class OrderPayMatchDetect extends KeyedProcessFunction<Long, OrderEvent, OrderResult> {

        // 定义状态， 保存之前点单是否已经来过create、 pay事件
        ValueState<Boolean> isPayedState;
        ValueState<Boolean> isCreatedState;
        // 定义状态， 保存定时器时间戳
        ValueState<Long> timerTsState;

        @Override
        public void open(Configuration parameters) throws Exception {
            isPayedState = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("is-payed", Boolean.class, false));
            isCreatedState = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("is-created", Boolean.class, false));
            timerTsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer-ts", Long.class));
        }

        @Override
        public void processElement(OrderEvent orderEvent, Context context, Collector<OrderResult> collector) throws Exception {
            // 先获取当前状态
            Boolean isPayed = isPayedState.value();
            Boolean isCreated = isCreatedState.value();
            Long timerTs = timerTsState.value();

            // 判断当前事件类型
            if ("create".equals(orderEvent.getOrderId())) {
                // 1. 如果来的是create， 要判断是否支付过
                if (isPayed) {
                    // 1.1 如果已经正常支付，输出正常匹配结果
                    collector.collect(new OrderResult(orderEvent.getOrderId(), "payed successfully"));
                    // 清空状态， 删除定时器
                    isPayedState.clear();
                    isCreatedState.clear();
                    timerTsState.clear();
                    context.timerService().deleteEventTimeTimer(timerTs);
                } else {
                    // 1.2 如果没有支付过， 注册15分钟后的定时器，开始等待支付事件
                    Long ts = (orderEvent.getTimestamp() + 15 * 60) * 1000L;
                    context.timerService().registerEventTimeTimer(ts);
                    // 更新状态
                    isCreatedState.update(true);
                    timerTsState.update(ts);
                }
            } else if ("pay".equals(orderEvent.getEventType())) {
                // 2. 如果来的是pay， 要判断是否有下单事件来过
                if (isCreated) {
                    // 2.1 已经有过下单事件， 要继续判断支付的时间戳是否超过15分钟
                    if (orderEvent.getTimestamp() * 1000L < timerTs) {
                        // 2.1.1 在15分钟内，没有超时， 正常匹配输出
                        collector.collect(new OrderResult(orderEvent.getOrderId(), "payed successfully"));
                    } else {
                        // 2.1.2 已经超时， 输出侧输出流报警
                        context.output(orderTimeoutTag, new OrderResult(orderEvent.getOrderId(), "payed but already timeout"));
                    }

                    // 统一清空状态, 删除定时器
                    isPayedState.clear();
                    isCreatedState.clear();
                    timerTsState.clear();
                    context.timerService().deleteEventTimeTimer(timerTs);
                } else {
                    // 2.2 没有下单事件， 乱序， 注册一个定时器， 等待下单事件
                    context.timerService().registerEventTimeTimer( orderEvent.getTimestamp() * 1000L);
                    // 更新状态
                    timerTsState.update(orderEvent.getTimestamp() * 1000L);
                    isPayedState.update(true);
                }
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<OrderResult> out) throws Exception {
            // 定时器触发，说明一定有一个事件没来
            if (isPayedState.value()) {
                // 如果pay来了， 说明create没来
                ctx.output(orderTimeoutTag, new OrderResult(ctx.getCurrentKey(), "payed but not found created log"));
            } else {
                // 如果pay没来，支付超时
                ctx.output(orderTimeoutTag, new OrderResult(ctx.getCurrentKey(), "timeout"));
            }
            // 清空状态
            isCreatedState.clear();
            isPayedState.clear();
            timerTsState.clear();
        }
    }
}
