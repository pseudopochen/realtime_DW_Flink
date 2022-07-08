package com.luban.micromall.realtime.app.ods;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.luban.micromall.realtime.app.function.CustomDeserialization;
import com.luban.micromall.realtime.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkCDC {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

//        env.setStateBackend(new FsStateBackend("hdfs://localhost:8020/micromall/ck"));
//
//        env.enableCheckpointing(5000L);
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(10000L);
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000);

        DebeziumSourceFunction<String> source = MySQLSource.<String>builder()
                .hostname("localhost")
                .port(3306)
                .username("root")
                .password("root")
                .databaseList("gmall")
                //.tableList("gmall.base_trademark")
                .deserializer(new CustomDeserialization())
                .startupOptions(StartupOptions.latest()) //initial())
                .build();

        DataStreamSource<String> streamSource = env.addSource(source);
        streamSource.print();
        String sinkTopic = "ods_base_db";
        streamSource.addSink(MyKafkaUtil.getKafkaProducer(sinkTopic));

        env.execute("ods.FlinkCDC");
    }

}
