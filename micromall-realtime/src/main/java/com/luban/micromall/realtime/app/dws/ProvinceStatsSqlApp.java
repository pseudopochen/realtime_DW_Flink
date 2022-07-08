package com.luban.micromall.realtime.app.dws;

import com.luban.micromall.realtime.bean.ProvinceStats;
import com.luban.micromall.realtime.utils.ClickHouseUtil;
import com.luban.micromall.realtime.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class ProvinceStatsSqlApp {
    public static void main(String[] args) throws Exception {
        //1. set up environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

//        env.setStateBackend(new FsStateBackend("hdfs://localhost:8020/micromall/ck"));
//
//        env.enableCheckpointing(5000L);
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(10000L);
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000);

        //2. DDL
        String groupId = "province_stats";
        String orderWideTopic = "dwm_order_wide";
        tableEnv.executeSql("CREATE TABLE ORDER_WIDE (province_id BIGINT, " +
                "province_name STRING,province_area_code STRING," +
                "province_iso_code STRING,province_3166_2_code STRING,order_id STRING, " +
                "total_amount DOUBLE,create_time STRING,rowtime AS TO_TIMESTAMP(create_time) ," +
                "WATERMARK FOR rowtime AS rowtime)" +
                " WITH (" + MyKafkaUtil.getKafkaDDL(orderWideTopic, groupId) + ")"
        );

        //3. query, keyBy, window, aggregate
        Table table = tableEnv.sqlQuery("select " +
                "DATE_FORMAT(TUMBLE_START(rowtime, INTERVAL '10' SECOND ),'yyyy-MM-dd HH:mm:ss') stt, " +
                "DATE_FORMAT(TUMBLE_END(rowtime, INTERVAL '10' SECOND ),'yyyy-MM-dd HH:mm:ss') edt , " +
                " province_id,province_name,province_area_code," +
                "province_iso_code,province_3166_2_code," +
                "COUNT( DISTINCT order_id) order_count, sum(total_amount) order_amount," +
                "UNIX_TIMESTAMP()*1000 ts " +
                " from ORDER_WIDE group by TUMBLE(rowtime, INTERVAL '10' SECOND )," +
                " province_id,province_name,province_area_code,province_iso_code,province_3166_2_code "
        );

        //4. convert to stream
        DataStream<ProvinceStats> provinceStatsDataStream = tableEnv.toAppendStream(table, ProvinceStats.class);

        //5. write into clickhouse
        provinceStatsDataStream.print();
        provinceStatsDataStream.addSink(ClickHouseUtil.getSink("insert into province_stats values(?,?,?,?,?,?,?,?,?,?)"));

        //6. execute
        env.execute("ProvinceStatsApp");

    }
}
