package com.luban;

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

import java.util.List;

public class CustomDeserialization implements DebeziumDeserializationSchema<String> {

    private JSONObject struct2json(Struct s) {
        JSONObject out = new JSONObject();
        Schema schema = s.schema();
        List<Field> fields = schema.fields();
        for (Field field : fields) {
            Object o = s.get(field);
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
