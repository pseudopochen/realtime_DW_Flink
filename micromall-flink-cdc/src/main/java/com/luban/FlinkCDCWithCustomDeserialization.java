package com.luban;


import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.alibaba.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkCDCWithCustomDeserialization {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        DebeziumSourceFunction<String> source = MySQLSource.<String>builder()
                .hostname("localhost")
                .port(3306)
                .username("root")
                .password("root")
                .databaseList("gmall")
                .tableList("gmall.base_trademark")
                .deserializer(new CustomDeserialization())
                .startupOptions(StartupOptions.initial())
                .build();

        DataStreamSource<String> streamSource = env.addSource(source);
        streamSource.print();

        env.execute("FlinkCDCWithCustomDeserialization");
    }
}
