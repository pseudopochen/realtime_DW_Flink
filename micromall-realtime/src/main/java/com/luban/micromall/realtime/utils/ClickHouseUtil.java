package com.luban.micromall.realtime.utils;

import com.luban.micromall.realtime.bean.TransientSink;
import com.luban.micromall.realtime.common.MicromallConfig;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class ClickHouseUtil {
    public static <T> SinkFunction<T> getSink(String sql) {
        return JdbcSink.<T>sink(sql, new JdbcStatementBuilder<T>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, T t) throws SQLException {
                        try {
                            Field[] fields = t.getClass().getDeclaredFields();
                            int i = 1;
                            for (Field field : fields) {
                                field.setAccessible(true);
                                TransientSink annotation = field.getAnnotation(TransientSink.class);
                                if (annotation != null) {
                                    continue;
                                }
                                Object o = field.get(t);
                                preparedStatement.setObject(i, o);
                                i++;
                            }
                        } catch (IllegalAccessException e) {
                            e.printStackTrace();
                        }

                    }
                }, new JdbcExecutionOptions.Builder()
                        .withBatchSize(5)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withDriverName(MicromallConfig.CLICKHOUSE_DRIVER)
                        .withUrl(MicromallConfig.CLICKHOUSE_URL)
                        .build());
    }
}
