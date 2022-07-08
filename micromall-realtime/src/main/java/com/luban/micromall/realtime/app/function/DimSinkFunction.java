package com.luban.micromall.realtime.app.function;

import com.alibaba.fastjson.JSONObject;
import com.luban.micromall.realtime.common.MicromallConfig;
import com.luban.micromall.realtime.utils.DimUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Set;

public class DimSinkFunction extends RichSinkFunction<JSONObject> {
    private Connection connection;

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(MicromallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(MicromallConfig.PHOENIX_SERVER);
        connection.setAutoCommit(true);
    }

    @Override
    public void invoke(JSONObject value, Context context) throws Exception {
        PreparedStatement preparedStatement = null;
        try {
            String sinkTable = value.getString("sinkTable");
            JSONObject after = value.getJSONObject("after");
            String upsertSQL = genUpsertSQL(sinkTable, after);
            preparedStatement = connection.prepareStatement(upsertSQL);
            if ("update".equals(value.get("type"))) {
                DimUtil.delRedisDimInfo(sinkTable, after.getString("id"));
            }
            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (preparedStatement != null) {
                preparedStatement.close();
            }
        }
    }

    private String genUpsertSQL(String sinkTable, JSONObject data) {
        Set<String> keySet = data.keySet();
        Collection<Object> values = data.values();
        return "upsert into " + MicromallConfig.HBASE_SCHEMA + "." + sinkTable + "(" +
                StringUtils.join(keySet, ",") + ") values ('" +
                StringUtils.join(values, "','") + "')"; // values must be surrounded by single-quotes
    }
}
