package com.myflink.hotitems_analysis;

import com.myflink.hotitems_analysis.beans.ItemViewCount;
import com.myflink.hotitems_analysis.beans.UserBehavior;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;

/**
 * • 抽取出业务时间戳，告诉 Flink 框架基于业务时间做窗口
 * • 过滤出点击行为数据
 * • 按一小时的窗口大小，每 5 分钟统计一次，做滑动窗口聚合(Sliding Window)
 * • 按每个窗口聚合，输出每个窗口中点击量前 N 名的商品
 *
 * @Author: xuzengfeng on 2021/7/6
 * @Version: 1.0
 * @Description:
 */
public class HotItems {

    public static void main(String[] args) throws Exception {

        // 1. 创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 2. 读取数据，创建DataStream
        DataStreamSource<String> inputStream = env.readTextFile("/Users/xuzengfeng/IdeaProjects/UserBehaviorAnalysis/HotItemsAnalysis/src/main/resources/UserBehavior.csv");

        // 3. 转换为PoJO，分配时间戳和watermark
        DataStream<UserBehavior> dataStream = inputStream.map(new MapFunction<String, UserBehavior>() {
            @Override
            public UserBehavior map(String value) throws Exception {
                String[] fields = value.split(",");
                return new UserBehavior(new Long(fields[0]), new Long(fields[1]), new Integer(fields[2]), fields[3], new Long(fields[4]));
            }
        })
                // 由于测试数据中的timestamp已经是排好序的，所以这里用Asc
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
                    @Override
                    public long extractAscendingTimestamp(UserBehavior element) {
                        return element.getTimestamp() * 1000L;
                    }
                });

        //增量聚合：本时间窗口范围内的数据聚合
        //全量聚合：本次时间窗口聚合结果+ 历史聚合结果 聚合后生成新的历史聚合结果

        // 4. 过滤处点击行为数据
        //  分组开窗聚合， 得到每个窗口内各个商品的count值
        DataStream<ItemViewCount> windowAggStream = dataStream
                .filter(data -> "pv".equals(data.getBehavior()))   // 过滤处点击行为
                .keyBy("itemId")
                .timeWindow(Time.hours(1), Time.minutes(5))  // 开滑动窗口
                .aggregate(new ItemCountAgg(), new WindowItemCountResult());// aggregate增量聚合

//        windowAggStream.print();

        // 5. 收集同一窗口的所有商品的count数据， 排序输出top n
        windowAggStream
                .keyBy("windowEnd")
                // 使用processFunction 更底层的API， 用到定时器
                .process(new TopNHotItems(5));  // 用自定义处理函数排序前5



        env.execute();


    }


    // 实现自定义增量聚合函数, 相当于一个预聚合的功能
    public static class ItemCountAgg implements AggregateFunction<UserBehavior, Long, Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(UserBehavior value, Long accumulator) {
            return accumulator + 1;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a + b;
        }
    }


    //  自定义全窗口函数
    public static class WindowItemCountResult implements WindowFunction<Long, ItemViewCount, Tuple, TimeWindow> {

        @Override
        public void apply(Tuple tuple, TimeWindow window, Iterable<Long> input, Collector<ItemViewCount> out) throws Exception {
//            System.out.println("----" + tuple);   // ----(3735496)
            Long itemId = tuple.getField(0);
            long windowEnd = window.getEnd();
            Long count = input.iterator().next();
            out.collect(new ItemViewCount(itemId, windowEnd, count));
        }
    }


    // 自定义实现KeyedProcessFunction
    // 键控状态
    public static class TopNHotItems extends KeyedProcessFunction<Tuple, ItemViewCount, String> {

        // 定义属性， top n的大小
        private Integer topSize;

        public TopNHotItems(Integer topSize) {
            this.topSize = topSize;
        }

        // 定义列表状态， 保存当前窗口所有输出的ItemViewCount
        ListState<ItemViewCount> itemViewCountListState;

        @Override
        public void open(Configuration parameters) throws Exception {
            itemViewCountListState = getRuntimeContext().getListState(new ListStateDescriptor<ItemViewCount>("item-view-count-list", ItemViewCount.class));
        }

        @Override
        public void processElement(ItemViewCount value, Context ctx, Collector<String> out) throws Exception {

            // 每来一条数据， 存入List中，并注册定时器
            itemViewCountListState.add(value);
            ctx.timerService().registerProcessingTimeTimer(value.getWindowEnd() + 1L);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            // 定时器触发，当前已收集到所有数据，排序输出
            ArrayList<ItemViewCount> itemViewCounts = Lists.newArrayList(itemViewCountListState.get().iterator());

            itemViewCounts.sort(new Comparator<ItemViewCount>() {
                @Override
                public int compare(ItemViewCount o1, ItemViewCount o2) {
                    return o2.getCount().intValue() - o1.getCount().intValue();
                }
            });

            // 将排名信息格式化成String， 方便打印输出
            StringBuilder resultBuilder = new StringBuilder();
            resultBuilder.append("===================================\n");
            resultBuilder.append("窗口结束时间：").append( new Timestamp(timestamp - 1)).append("\n");

            // 遍历列表， 取top n输出
            for (int i = 0; i < Math.min(topSize, itemViewCounts.size()); i++) {
                ItemViewCount currentItemViewCount = itemViewCounts.get(i);
                resultBuilder.append("NO ").append(i+1).append(":")
                        .append(" 商品ID = ").append(currentItemViewCount.getItemId())
                        .append(" 热门度 = ").append(currentItemViewCount.getCount())
                        .append("\n");
            }
            resultBuilder.append("===============================\n\n");

            // 控制输出频率
            Thread.sleep(1000L);
            out.collect(resultBuilder.toString());

        }
    }

}
