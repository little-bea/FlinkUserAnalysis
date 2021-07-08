package com.myflink.hotitems_analysis;

import com.myflink.hotitems_analysis.beans.UserBehavior;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Slide;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.Properties;

/**
 * @Author: xuzengfeng on 2021/7/8
 * @Version: 1.0
 * @Description:
 */
public class HotItemsWithSql {

    public static void main(String[] args) throws Exception {

        // 1. 创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 2. 读取文件数据，创建DataStream
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

        // 4. 创建表执行环境，用blink版本
        // pom 文件中引入相关依赖
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        // 5. 将流转换成表
        Table dataTable = tableEnv.fromDataStream(dataStream, "itemId, behavior, timestamp.rowtime as ts");

        // 6. 分组开窗
        Table windowAggTable = dataTable
                .filter("behavior = 'pv'")
                .window(Slide.over("1.hours").every("5.minutes").on("ts").as("w"))
                .groupBy("itemId, w")
                .select("itemId, w.end as windowEnd, itemId.count as cnt");

        // 7. 利用开窗函数，对count值进行排序并获取Row number， 得到Top N
        // SQL
        DataStream<Row> aggStream = tableEnv.toAppendStream(windowAggTable, Row.class);
        tableEnv.createTemporaryView("agg", aggStream, "itemId, windowEnd, cnt");

        Table resultTable = tableEnv.sqlQuery("select * from " +
                " (select *, ROW_NUMBER() over (partition by windowEnd order by cnt desc) as row_num" +
                " from agg)" +
                " where row_num <= 5");

//        tableEnv.toRetractStream(resultTable, Row.class).print();

        // 纯SQL实现
        tableEnv.createTemporaryView("data_table", dataStream, "itemId, behavior, timestamp.rowtime as ts");
        Table resultSqlTable = tableEnv.sqlQuery("select * from " +
                " (select *, ROW_NUMBER() over (partition by windowEnd order by cnt desc) as row_num" +
                " from (" +
                "   select itemId, count(itemId) as cnt, HOP_END(ts, interval '5' minute, interval '1' hour) as windowEnd " +
                "   from data_table " +
                "   where behavior = 'pv'" +
                "   group by itemId, HOP(ts, interval '5' minute, interval '1' hour)" +
                "   )" +
                " )" +
                " where row_num <= 5");

        tableEnv.toRetractStream(resultSqlTable, Row.class).print();

        env.execute("hot items with sql job");

    }
}
