package com.luban.micromall.realtime.app.function;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.luban.micromall.realtime.bean.TableProcess;
import com.luban.micromall.realtime.common.MicromallConfig;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {

    private OutputTag<JSONObject> objectOutputTag;
    private MapStateDescriptor<String, TableProcess> mapStateDescriptor;
    private Connection connection;

    public TableProcessFunction(OutputTag<JSONObject> objectOutputTag, MapStateDescriptor<String, TableProcess> mapStateDescriptor) {
        this.objectOutputTag = objectOutputTag;
        this.mapStateDescriptor = mapStateDescriptor;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(MicromallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(MicromallConfig.PHOENIX_SERVER);
    }

    @Override
    public void processElement(JSONObject jsonObject, BroadcastProcessFunction<JSONObject, String, JSONObject>.ReadOnlyContext readOnlyContext, Collector<JSONObject> collector) throws Exception {

        //1. get state
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = readOnlyContext.getBroadcastState(mapStateDescriptor);
        String key = jsonObject.getString("tableName") + "-" + jsonObject.getString("type");
        TableProcess tableProcess = broadcastState.get(key);
        if (tableProcess != null) {

            //2. filter fields (columns)
            JSONObject data = jsonObject.getJSONObject("after");
            filterColumn(data, tableProcess.getSinkColumns());

            //3. split the flow
            jsonObject.put("sinkTable", tableProcess.getSinkTable());
            String sinkType = tableProcess.getSinkType();
            if (TableProcess.SINK_TYPE_KAFKA.equals(sinkType)) { // kafka stream
                collector.collect(jsonObject);
            } else if (TableProcess.SINK_TYPE_HBASE.equals(sinkType)) { // hbase stream
                readOnlyContext.output(objectOutputTag, jsonObject);
            }

        } else {
            System.out.println("key : " + key + " does not exist!");
        }
    }

    private void filterColumn(JSONObject data, String sinkColumns) {
        List<String> columns = Arrays.asList(sinkColumns.split(","));
        //        while (iterator.hasNext()) {
//            if (!columns.contains(iterator.next().getKey())) {
//                iterator.remove();
//            }
//        }
        data.entrySet().removeIf(stringObjectEntry -> !columns.contains(stringObjectEntry.getKey()));
    }

    @Override
    public void processBroadcastElement(String s, BroadcastProcessFunction<JSONObject, String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
        //1. get and process data
        JSONObject jsonObject = JSON.parseObject(s);
        String data = jsonObject.getString("after");
        TableProcess tableProcess = JSON.parseObject(data, TableProcess.class);

        //2. create hbase table
        if (TableProcess.SINK_TYPE_HBASE.equals(tableProcess.getSinkType())) {
            checkTable(tableProcess.getSinkTable(), tableProcess.getSinkColumns(), tableProcess.getSinkPk(), tableProcess.getSinkExtend());
        }

        //3. write into state and broadcast
        BroadcastState<String, TableProcess> broadcastState = context.getBroadcastState(mapStateDescriptor);
        String key = tableProcess.getSourceTable() + "-" + tableProcess.getOperateType();
        broadcastState.put(key, tableProcess);
    }

    private void checkTable(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend) {
        PreparedStatement preparedStatement = null;
        try {
            if (sinkPk == null) {
                sinkPk = "id";
            }
            if (sinkExtend == null) {
                sinkExtend = "";
            }
            StringBuffer createTableSQL = new StringBuffer("create table if not exists ")
                    .append(MicromallConfig.HBASE_SCHEMA)
                    .append(".")
                    .append(sinkTable)
                    .append("(");

            String[] fields = sinkColumns.split(",");
            for (int i = 0; i < fields.length; i++) {
                String field = fields[i];
                if (sinkPk.equals(field)) {
                    createTableSQL.append(field).append(" varchar primary key ");
                } else {
                    createTableSQL.append(field).append(" varchar ");
                }
                if (i < fields.length - 1) {
                    createTableSQL.append(",");
                }
            }
            createTableSQL.append(")").append(sinkExtend);
            System.out.println(createTableSQL);

            // prepare SQL
            preparedStatement = connection.prepareStatement(createTableSQL.toString());
            preparedStatement.execute();
        } catch (SQLException e) {
            throw new RuntimeException("Error creating table: " + sinkTable);
        } finally {
            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
        }

    }
}
