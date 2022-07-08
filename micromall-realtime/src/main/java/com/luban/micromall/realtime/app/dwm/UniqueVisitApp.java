package com.luban.micromall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.luban.micromall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.text.SimpleDateFormat;

public class UniqueVisitApp {
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

        //2. read kafka dwd_page_log topic
        String sourceTopic = "dwd_page_log";
        String groupId = "unique_visit_app";
        String sinkTopic = "dwm_unique_visit";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getKafkaConsumer(sourceTopic, groupId));

        //3. convert to json
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(JSON::parseObject);

        //4. filter data using state, keep only the first visit of each mid
        KeyedStream<JSONObject, String> keyedStream = jsonObjDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));
        SingleOutputStreamOperator<JSONObject> uvDS = keyedStream.filter(new RichFilterFunction<JSONObject>() {
            private ValueState<String> dateState;
            private SimpleDateFormat simpleDateFormat;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("date-state", String.class);
                StateTtlConfig ttlConfig = new StateTtlConfig.Builder(Time.hours(24)).setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite).build();
                valueStateDescriptor.enableTimeToLive(ttlConfig);
                dateState = getRuntimeContext().getState(valueStateDescriptor);
                simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
            }

            @Override
            public boolean filter(JSONObject jsonObject) throws Exception {
                String lastPageId = jsonObject.getJSONObject("page").getString("last_page_id");
                if (lastPageId == null || lastPageId.length() <= 0) {
                    String lastDate = dateState.value();
                    String currDate = simpleDateFormat.format(jsonObject.getLong("ts"));
                    if (!currDate.equals(lastDate)) {
                        dateState.update(currDate);
                        return true;
                    } else {
                        return false;
                    }
                } else {
                    return false;
                }

            }
        });

        //5. write to kafka dwm layer
        uvDS.print();
        uvDS.map(json -> json.toJSONString())
                .addSink(MyKafkaUtil.getKafkaProducer(sinkTopic));

        //6. execute
        env.execute("UniqueVisitApp");
    }
}
