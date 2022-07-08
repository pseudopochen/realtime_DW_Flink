package com.luban.micromall.realtime.app.function;

import com.alibaba.fastjson.JSONObject;

import java.text.ParseException;

public interface DimAsyncJoinFunction<T> {

    void join(T t, JSONObject dimInfo) throws ParseException;

    String getKey(T t);
}
