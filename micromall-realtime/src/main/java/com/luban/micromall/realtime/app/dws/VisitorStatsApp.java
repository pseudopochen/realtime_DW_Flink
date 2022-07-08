package com.luban.micromall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.luban.micromall.realtime.bean.VisitorStats;
import com.luban.micromall.realtime.utils.ClickHouseUtil;
import com.luban.micromall.realtime.utils.DateTimeUtil;
import com.luban.micromall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;


import java.time.Duration;
import java.util.Date;

public class VisitorStatsApp {
    public static void main(String[] args) throws Exception {
        //1. set up environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

//        env.setStateBackend(new FsStateBackend("hdfs://localhost:8020/micromall/ck"));
//
//        env.enableCheckpointing(5000L);
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(10000L);
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000);

        //2. read kafka
        String groupId = "visitor_stats_app";

        String uniqueVisitSourceTopic = "dwm_unique_visit";
        String userJumpDetailSourceTopic = "dwm_user_jump_detail";
        String pageViewSourceTopic = "dwd_page_log";
        DataStreamSource<String> uvDS = env.addSource(MyKafkaUtil.getKafkaConsumer(uniqueVisitSourceTopic, groupId));
        DataStreamSource<String> ujDS = env.addSource(MyKafkaUtil.getKafkaConsumer(userJumpDetailSourceTopic, groupId));
        DataStreamSource<String> pvDS = env.addSource(MyKafkaUtil.getKafkaConsumer(pageViewSourceTopic, groupId));

        //3. convert all streams to the same VisitorStats
        SingleOutputStreamOperator<VisitorStats> visitorStatsWithUvDS = uvDS.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);
            JSONObject common = jsonObject.getJSONObject("common");

            return new VisitorStats("", "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    1L, 0L, 0L, 0L, 0L,
                    jsonObject.getLong("ts"));
        });

        SingleOutputStreamOperator<VisitorStats> visitorStatsWithUjDS = ujDS.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);
            JSONObject common = jsonObject.getJSONObject("common");

            return new VisitorStats("", "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    0L, 0L, 0L, 1L, 0L,
                    jsonObject.getLong("ts"));
        });

        SingleOutputStreamOperator<VisitorStats> visitorStatsWithPvDS = pvDS.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);
            JSONObject common = jsonObject.getJSONObject("common");
            JSONObject page = jsonObject.getJSONObject("page");
            String last_page_id = page.getString("last_page_id");
            long sv = 0L;
            if (last_page_id == null || last_page_id.length() <= 0) {
                sv = 1L;
            }
            return new VisitorStats("", "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    0L, 1L, sv, 0L, page.getLong("during_time"),
                    jsonObject.getLong("ts"));
        });

        //4. union all streams
        DataStream<VisitorStats> unionDS = visitorStatsWithUvDS.union(visitorStatsWithUjDS, visitorStatsWithPvDS);

        //5. timestamp and watermark
        SingleOutputStreamOperator<VisitorStats> visitorStatsWithWMDS = unionDS
                .assignTimestampsAndWatermarks(WatermarkStrategy.<VisitorStats>forBoundedOutOfOrderness(Duration.ofSeconds(11))
                .withTimestampAssigner(new SerializableTimestampAssigner<VisitorStats>() {
                    @Override
                    public long extractTimestamp(VisitorStats visitorStats, long l) {
                        return visitorStats.getTs();
                    }
                }));

        //6. keyBy
        KeyedStream<VisitorStats, Tuple4<String, String, String, String>> keyedStream = visitorStatsWithWMDS.keyBy(new KeySelector<VisitorStats, Tuple4<String, String, String, String>>() {
            @Override
            public Tuple4<String, String, String, String> getKey(VisitorStats visitorStats) throws Exception {
                return new Tuple4<String, String, String, String>(visitorStats.getAr(),
                        visitorStats.getCh(),
                        visitorStats.getIs_new(),
                        visitorStats.getVc());
            }
        });

        //7. window and aggregate
        WindowedStream<VisitorStats, Tuple4<String, String, String, String>, TimeWindow> windowStream = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(10L)));
        SingleOutputStreamOperator<VisitorStats> result = windowStream.reduce(new ReduceFunction<VisitorStats>() {
            @Override
            public VisitorStats reduce(VisitorStats value1, VisitorStats value2) throws Exception {
                value1.setUv_ct(value1.getUv_ct() + value2.getUv_ct()); // only works for tumbling window, sliding window will need to new a VisitorStats
                value1.setPv_ct(value1.getPv_ct() + value2.getPv_ct());
                value1.setSv_ct(value1.getSv_ct() + value2.getSv_ct());
                value1.setUj_ct(value1.getUj_ct() + value2.getUj_ct());
                value1.setDur_sum(value1.getDur_sum() + value2.getDur_sum());
                return value1;
            }
        }, new WindowFunction<VisitorStats, VisitorStats, Tuple4<String, String, String, String>, TimeWindow>() {
            @Override
            public void apply(Tuple4<String, String, String, String> stringStringStringStringTuple4, TimeWindow timeWindow, Iterable<VisitorStats> iterable, Collector<VisitorStats> collector) throws Exception {
                long start = timeWindow.getStart();
                long end = timeWindow.getEnd();
                VisitorStats visitorStats = iterable.iterator().next();
                visitorStats.setStt(DateTimeUtil.toYMDhms(new Date(start)));
                visitorStats.setEdt(DateTimeUtil.toYMDhms(new Date(end)));
                collector.collect(visitorStats);
            }
        });

        //8. write into ClickHouse
        result.print(">>>>>>>>>>>");
        result.addSink(ClickHouseUtil.getSink("insert into visitor_stats values(?,?,?,?,?,?,?,?,?,?,?,?)"));

        //9. execute
        env.execute("VisitorStatsApp");
    }
}
