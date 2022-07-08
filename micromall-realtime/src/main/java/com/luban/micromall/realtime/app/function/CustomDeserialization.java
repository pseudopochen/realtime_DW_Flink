package com.luban.micromall.realtime.app.function;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

public class CustomDeserialization implements DebeziumDeserializationSchema<String> {

    private JSONObject struct2json(Struct s) {
        JSONObject out = new JSONObject();
        Schema schema = s.schema();
        //System.out.println("schema: " + schema);
        //System.out.println("Struct: " + s);
        List<Field> fields = schema.fields();
        for (Field field : fields) {

            Object o = s.get(field);

            // https://blog.csdn.net/qq_31866793/article/details/121246900
            if ("birthday".equals(field.name())) {
                Date d = new Date();
                d.setTime((int)o * 24 * 3600 * 1000L);
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                String dateStr = sdf.format(d);
                o = dateStr;
                //System.out.println("birthday: " + o + " struct: " + s);
                //System.out.println("birthday schema: " + field.schema().type().getName() + " dateStr: " + dateStr);
            }

            out.put(field.name(), o);
        }
        return out;
    }

    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
        /**
         * 封装的数据格式
         * {
         * "database":"",
         * "tableName":"",
         * "before":{"id":"","tm_name":""....},
         * "after":{"id":"","tm_name":""....},
         * "type":"c u d",
         * //"ts":156456135615
         * }
         */
        JSONObject result = new JSONObject();
        String topic = sourceRecord.topic();
        String[] fields = topic.split("\\.");
        String database = fields[1];
        String tableName = fields[2];
        result.put("database", database);
        result.put("tableName", tableName);

        Struct value = (Struct)sourceRecord.value();
//        if ("user_info".equals(tableName)) {
//            System.out.println("sourceRecord >>>>>>>>>>> " + sourceRecord);
//        }
        Struct before = value.getStruct("before");
        if (before != null) {
            result.put("before", struct2json(before));
        } else {
            result.put("before", new JSONObject());
        }

        Struct after = value.getStruct("after");
        if (after != null) {
            result.put("after", struct2json(after));
        } else {
            result.put("after", new JSONObject());
        }

        Envelope.Operation operation = Envelope.operationFor(sourceRecord);
        String type = operation.toString().toLowerCase();
        if ("create".equals(type)) {
            type = "insert";
        }
        result.put("type", type);

        collector.collect(result.toJSONString());
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}
