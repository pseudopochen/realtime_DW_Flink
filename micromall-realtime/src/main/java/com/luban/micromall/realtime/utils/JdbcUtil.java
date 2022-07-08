package com.luban.micromall.realtime.utils;

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.CaseFormat;
import com.luban.micromall.realtime.common.MicromallConfig;
import org.apache.commons.beanutils.BeanUtils;
import org.codehaus.jackson.map.util.BeanUtil;

import java.lang.reflect.InvocationTargetException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class JdbcUtil {
    public static <T> List<T> queryList(Connection connection, String querySql, Class<T> clz, boolean underScoreToCamel) throws SQLException, InstantiationException, IllegalAccessException, InvocationTargetException {
        ArrayList<T> resultList = new ArrayList<>();

        PreparedStatement preparedStatement = connection.prepareStatement(querySql);
        ResultSet resultSet = preparedStatement.executeQuery();
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();
        while (resultSet.next()) {
            T t = clz.newInstance();
            for (int i = 1; i < columnCount + 1; i++) {
                String columnName = metaData.getColumnName(i);
                if (underScoreToCamel) {
                    columnName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columnName.toLowerCase());
                }
                Object value = resultSet.getObject(i);
                BeanUtils.setProperty(t, columnName, value);
            }
            resultList.add(t);
        }
        preparedStatement.close();
        resultSet.close();
        return resultList;
    }

    public static void main(String[] args) throws Exception {
        Class.forName(MicromallConfig.PHOENIX_DRIVER);
        Connection connection = DriverManager.getConnection(MicromallConfig.PHOENIX_SERVER);
        List<JSONObject> queryList = queryList(connection, "select * from GMALL_REALTIME.DIM_USER_INFO", JSONObject.class, true);
        for (JSONObject jsonObject : queryList) {
            System.out.println(jsonObject);
        }

        connection.close();
    }
}
