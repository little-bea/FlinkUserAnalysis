package com.myflink.networkflow_analysis;

import com.myflink.networkflow_analysis.beans.ApacheLogEvent;
import com.myflink.networkflow_analysis.beans.PageViewCount;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * @author xuzengfeng
 * @description
 */
public class HotPages {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 读取文件，转换成POJO
        DataStream<String> inputStream = env.readTextFile("G:\\MyProject\\FlinkUserAnalysis\\NetworkFlowAnalysis\\src\\main\\resources\\apache.log");

        DataStream<ApacheLogEvent> dataStream = inputStream
                .map(line -> {
                    String[] fields = line.split(" ");
                    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss");
                    Long timestamp = simpleDateFormat.parse(fields[3]).getTime();
                    return new ApacheLogEvent(fields[0], fields[1], timestamp, fields[5], fields[6]);
                })
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<ApacheLogEvent>(Time.seconds(1)) {
                    @Override
                    public long extractTimestamp(ApacheLogEvent apacheLogEvent) {
                        return apacheLogEvent.getTimestamp();
                    }
                });

//        dataStream.print("data");



        //过滤，分组开窗聚合
        // 定义一个侧输出流标签
        OutputTag<ApacheLogEvent> lateTag = new OutputTag<ApacheLogEvent>("late") {};

        SingleOutputStreamOperator<PageViewCount> windowAggStream = dataStream
                .filter(data -> "GET".equals(data.getMethod()))   // 过滤出get请求
                .filter(data -> {
                    String regex = "^((?!\\.(css|js|png|ico)$).)*$";
                    return Pattern.matches(regex, data.getUrl());
                })
                .keyBy(ApacheLogEvent::getUrl)   // 按照url分组
                .timeWindow(Time.minutes(10), Time.seconds(5))
                .allowedLateness(Time.minutes(1))
                .sideOutputLateData(lateTag)
                .aggregate(new PageCountAgg(), new PageCountResult());

//        windowAggStream.print("agg");
//        windowAggStream.getSideOutput(lateTag).print("late");

        // 收集同一窗口count数据， 排序输出
        DataStream<String> resultStream = windowAggStream
                .keyBy(PageViewCount::getWindowEnd)
                .process(new TopNHotPages(3));

        resultStream.print();

        env.execute();
    }

    public static class PageCountAgg implements AggregateFunction<ApacheLogEvent, Long, Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(ApacheLogEvent apacheLogEvent, Long accumulator) {
            return accumulator + 1L;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long acc1, Long acc2) {
            return acc1 + acc2;
        }
    }

    // 实现自定义的窗口函数
    public static class PageCountResult implements WindowFunction<Long, PageViewCount, String, TimeWindow> {

        @Override
        public void apply(String url, TimeWindow timeWindow, Iterable<Long> input, Collector<PageViewCount> collector) throws Exception {
            collector.collect(new PageViewCount(url, timeWindow.getEnd(), input.iterator().next()));
        }
    }


    // 实现自定义的处理函数
    public static class TopNHotPages extends KeyedProcessFunction<Long, PageViewCount, String> {

        private Integer topSize;

        public TopNHotPages(Integer topSize) {
            this.topSize = topSize;
        }

        // 定义状态， 保存当前所有PageViewCount到Map中
        MapState<String, Long> pageViewCountMapState;

        @Override
        public void open(Configuration parameters) throws Exception {
            pageViewCountMapState = getRuntimeContext().getMapState(new MapStateDescriptor<String, Long>("page-count-map", String.class, Long.class));
        }

        @Override
        public void processElement(PageViewCount pageViewCount, Context context, Collector<String> collector) throws Exception {
            pageViewCountMapState.put(pageViewCount.getUrl(), pageViewCount.getCount());
            context.timerService().registerEventTimeTimer(pageViewCount.getWindowEnd() + 1);
            // 注册一个一分钟之后的定时器，用来清空状态
            context.timerService().registerEventTimeTimer(pageViewCount.getWindowEnd() + 60 * 1000L);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            // 先判断是否到了窗口关闭清理时间，如果是，直接清空状态返回
            if( timestamp == ctx.getCurrentKey() + 60 * 1000L ){
                pageViewCountMapState.clear();
                return;
            }

            ArrayList<Map.Entry<String, Long>> pageViewCounts = Lists.newArrayList(pageViewCountMapState.entries());

            pageViewCounts.sort(new Comparator<Map.Entry<String, Long>>() {
                @Override
                public int compare(Map.Entry<String, Long> o1, Map.Entry<String, Long> o2) {
                    return o2.getValue().intValue() - o1.getValue().intValue();  // 降序
                }
            });

            // 格式化成String输出
            StringBuilder resultBuilder = new StringBuilder();
            resultBuilder.append("===================================\n");
            resultBuilder.append("窗口结束时间：").append(new Timestamp(timestamp - 1)).append("\n");

            // 遍历列表，取top n输出
            for (int i = 0; i < Math.min(topSize, pageViewCounts.size()); i++) {
                Map.Entry<String, Long> currentItemViewCount = pageViewCounts.get(i);
                resultBuilder.append("NO ").append(i + 1).append(":")
                        .append(" 页面URL = ").append(currentItemViewCount.getKey())
                        .append(" 浏览量 = ").append(currentItemViewCount.getValue())
                        .append("\n");
            }
            resultBuilder.append("===============================\n\n");

            // 控制输出频率
            Thread.sleep(1000L);

            out.collect(resultBuilder.toString());
        }
    }
}
