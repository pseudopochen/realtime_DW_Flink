package com.luban.micromall.realtime.app.function;

import com.alibaba.fastjson.JSONObject;
import com.luban.micromall.realtime.bean.OrderWide;
import com.luban.micromall.realtime.common.MicromallConfig;
import com.luban.micromall.realtime.utils.DimUtil;
import com.luban.micromall.realtime.utils.ThreadPoolUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.Collections;
import java.util.concurrent.ThreadPoolExecutor;


public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T, T>  implements DimAsyncJoinFunction<T> {
    private Connection connection;
    private ThreadPoolExecutor threadPoolExecutor;
    private String tableName;

    public DimAsyncFunction(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(MicromallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(MicromallConfig.PHOENIX_SERVER);
        threadPoolExecutor = ThreadPoolUtil.getThreadPool();
    }

    @Override
    public void asyncInvoke(T t, ResultFuture<T> resultFuture) throws Exception {
        threadPoolExecutor.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    //get the primary key
                    String id = getKey(t);
                    //get dimension data
                    JSONObject dimInfo = DimUtil.getDimInfo(connection, tableName, id);
                    //add dimension data to wide table
                    if (dimInfo != null) {
                        join(t, dimInfo);
                    }
                    //write out
                    resultFuture.complete(Collections.singletonList(t));

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
    }

    @Override
    public void timeout(T input, ResultFuture<T> resultFuture) throws Exception {
        System.out.println("Timeout: " + input);
    }
}
