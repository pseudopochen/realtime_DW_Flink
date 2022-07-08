package com.luban.micromall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.luban.micromall.realtime.bean.OrderWide;
import com.luban.micromall.realtime.bean.PaymentInfo;
import com.luban.micromall.realtime.bean.PaymentWide;
import com.luban.micromall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;

public class PaymentWideApp {
    public static void main(String[] args) throws Exception {
        //1. set up environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

//        env.setStateBackend(new FsStateBackend("hdfs://localhost:8020/micromall/ck"));
//
//        env.enableCheckpointing(5000L);
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(10000L);
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000);

        //2.
        String groupId = "payment_wide_group";
        String paymentInfoSourceTopic = "dwd_payment_info";
        String orderWideSourceTopic = "dwm_order_wide";
        String paymentWideSinkTopic = "dwm_payment_wide";
        SingleOutputStreamOperator<OrderWide> orderWideDS = env.addSource(MyKafkaUtil.getKafkaConsumer(orderWideSourceTopic, groupId))
                .map(line -> {
                    return JSON.parseObject(line, OrderWide.class);
                }).assignTimestampsAndWatermarks(WatermarkStrategy.<OrderWide>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<OrderWide>() {
                    @Override
                    public long extractTimestamp(OrderWide orderWide, long l) {
                        //SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        try {
                            return new Long(orderWide.getCreate_time()); //sdf.parse(orderWide.getCreate_time()).getTime();
                        } catch (Exception e) {
                            e.printStackTrace();
                            return l;
                        }
                    }
                }));
        SingleOutputStreamOperator<PaymentInfo> paymentInfoDS = env.addSource(MyKafkaUtil.getKafkaConsumer(paymentInfoSourceTopic, groupId))
                .map(line -> {
                    return JSON.parseObject(line, PaymentInfo.class);
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<PaymentInfo>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<PaymentInfo>() {
                    @Override
                    public long extractTimestamp(PaymentInfo paymentInfo, long l) {
                        //SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        try {
                            return new Long(paymentInfo.getCreate_time()); //sdf.parse(paymentInfo.getCreate_time()).getTime();
                        } catch (Exception e) {
                            e.printStackTrace();
                            return l;
                        }
                    }
                }));

        //3. join
        SingleOutputStreamOperator<PaymentWide> paymentWideDS = paymentInfoDS.keyBy(PaymentInfo::getOrder_id)
                .intervalJoin(orderWideDS.keyBy(OrderWide::getOrder_id))
                .between(Time.minutes(-15), Time.seconds(5))
                .process(new ProcessJoinFunction<PaymentInfo, OrderWide, PaymentWide>() {
                    @Override
                    public void processElement(PaymentInfo paymentInfo, OrderWide orderWide, ProcessJoinFunction<PaymentInfo, OrderWide, PaymentWide>.Context context, Collector<PaymentWide> collector) throws Exception {
                        collector.collect(new PaymentWide(paymentInfo, orderWide));
                    }
                });

        //4. write into kafka
        paymentWideDS.print(">>>>>>>>>>>>>>>");
        paymentWideDS.map(JSONObject::toJSONString)
                        .addSink(MyKafkaUtil.getKafkaProducer(paymentWideSinkTopic));

        //5.
        env.execute("PaymentWideApp");
    }
}
