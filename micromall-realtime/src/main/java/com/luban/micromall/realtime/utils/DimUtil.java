package com.luban.micromall.realtime.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.luban.micromall.realtime.common.MicromallConfig;
import redis.clients.jedis.Jedis;

import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;

public class DimUtil {
    public static JSONObject getDimInfo(Connection connection, String tableName, String id) throws SQLException, InvocationTargetException, InstantiationException, IllegalAccessException {
        // check redis before accessing phoenix
        Jedis jedis = RedisUtil.getJedis();
        String redisKey = "DIM:" + tableName + ":" + id;
        String dimInfoJsonStr = jedis.get(redisKey);
        if (dimInfoJsonStr != null) {
            jedis.expire(redisKey, 24 * 3600);
            jedis.close();
            return JSONObject.parseObject(dimInfoJsonStr);
        }

        String querySql = "select * from " + MicromallConfig.HBASE_SCHEMA + "." + tableName + " where id = '" + id + "'"; // don't forget the single quote around id
        List<JSONObject> jsonObjects = JdbcUtil.queryList(connection, querySql, JSONObject.class, true);
        JSONObject dimInfoJson = jsonObjects.get(0);
        jedis.set(redisKey, dimInfoJson.toJSONString());
        jedis.expire(redisKey, 24 * 3600);
        jedis.close();
        return dimInfoJson;
    }

    public static void delRedisDimInfo(String tableName, String id) {
        Jedis jedis = RedisUtil.getJedis();
        String redisKey = "DIM:" + tableName + ":" + id;
        jedis.del(redisKey);
        jedis.close();
    }

    public static void main(String[] args) throws Exception {
        Class.forName(MicromallConfig.PHOENIX_DRIVER);
        Connection connection = DriverManager.getConnection(MicromallConfig.PHOENIX_SERVER);
        System.out.println(getDimInfo(connection, "DIM_USER_INFO", "79"));
        connection.close();
    }
}
