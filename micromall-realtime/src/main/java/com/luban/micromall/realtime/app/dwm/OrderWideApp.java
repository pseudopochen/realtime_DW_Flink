package com.luban.micromall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.luban.micromall.realtime.app.function.DimAsyncFunction;
import com.luban.micromall.realtime.bean.OrderDetail;
import com.luban.micromall.realtime.bean.OrderInfo;
import com.luban.micromall.realtime.bean.OrderWide;
import com.luban.micromall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.concurrent.TimeUnit;

public class OrderWideApp {
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

        //2. read kafka dwd_page_log topic
        String orderInfoSourceTopic = "dwd_order_info";
        String orderDetailSourceTopic = "dwd_order_detail";
        String groupId = "order_wide_group_0326";
        String orderWideSinkTopic = "dwm_order_wide";

        SingleOutputStreamOperator<OrderInfo> orderInfoDS = env.addSource(MyKafkaUtil.getKafkaConsumer(orderInfoSourceTopic, groupId))
                .map(line -> {
                    //System.out.println("orderInfo: " + line);
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

                    OrderInfo orderInfo = JSON.parseObject(line, OrderInfo.class);
                    String createTime = sdf.format(new Long(orderInfo.getCreate_time()));
                    //System.out.println("after format>>>>>>>>>>>>" + createTime);

                    String[] dateTimeArr = createTime.split(" ");

                    orderInfo.setCreate_date(dateTimeArr[0]);
                    orderInfo.setCreate_hour(dateTimeArr[1].split(":")[0]);

                    orderInfo.setCreate_ts(sdf.parse(createTime).getTime());
                    return orderInfo;
                }).assignTimestampsAndWatermarks(WatermarkStrategy.<OrderInfo>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<OrderInfo>() {
                    @Override
                    public long extractTimestamp(OrderInfo orderInfo, long l) {
                        return orderInfo.getCreate_ts();
                    }
                }));

        SingleOutputStreamOperator<OrderDetail> orderDetailDS = env.addSource(MyKafkaUtil.getKafkaConsumer(orderDetailSourceTopic, groupId))
                .map(line -> {
                    //System.out.println(line);
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    OrderDetail orderDetail = JSON.parseObject(line, OrderDetail.class);
                    String create_time = sdf.format(new Long(orderDetail.getCreate_time()));

                    orderDetail.setCreate_ts(sdf.parse(create_time).getTime());
                    return orderDetail;
                }).assignTimestampsAndWatermarks(WatermarkStrategy.<OrderDetail>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<OrderDetail>() {
                    @Override
                    public long extractTimestamp(OrderDetail orderDetail, long l) {
                        return orderDetail.getCreate_ts();
                    }
                }));

        //3. join the two DS
        SingleOutputStreamOperator<OrderWide> orderWideWithoutDimDS = orderInfoDS.keyBy(OrderInfo::getId)
                .intervalJoin(orderDetailDS.keyBy(OrderDetail::getOrder_id))
                .between(Time.seconds(-5), Time.seconds(5))
                .process(new ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>() {
                    @Override
                    public void processElement(OrderInfo orderInfo, OrderDetail orderDetail, ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>.Context context, Collector<OrderWide> collector) throws Exception {
                        collector.collect(new OrderWide(orderInfo, orderDetail));
                    }
                });
        orderWideWithoutDimDS.print("orderWideWithoutDimDS>>>>>>>>>>>>>>>>");

        //4. join the DIM
        //4.1 user_info
        SingleOutputStreamOperator<OrderWide> orderWideWithUserDS = AsyncDataStream.unorderedWait(
                orderWideWithoutDimDS, new DimAsyncFunction<OrderWide>("DIM_USER_INFO") {
                    @Override
                    public String getKey(OrderWide orderWide) {
                        return orderWide.getUser_id().toString();
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfo) throws ParseException {
                        orderWide.setUser_gender(dimInfo.getString("gender"));
                        String birthday = dimInfo.getString("birthday");
                        //System.out.println("dimInfo>>>>>>>>> " + dimInfo + " birthday>>>>>>>>>>>> " + birthday + " gender>>>>>>>>>>> " + dimInfo.getString("gender"));
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                        long age = (System.currentTimeMillis() - sdf.parse(birthday).getTime()) / (1000L * 60 * 60 * 24 * 365);
                        orderWide.setUser_age((int) age);
                    }
                }, 60, TimeUnit.SECONDS);
        //orderWideWithUserDS.print("orderWideWithUserDS");

        //4.2 base_province
        SingleOutputStreamOperator<OrderWide> orderWideWithProvinceDS = AsyncDataStream.unorderedWait(
                orderWideWithUserDS, new DimAsyncFunction<OrderWide>("DIM_BASE_PROVINCE") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfo) throws ParseException {
                        //System.out.println("province info: " + dimInfo + " area_code: " + dimInfo.getString("areaCode"));
                        orderWide.setProvince_name(dimInfo.getString("name"));
                        orderWide.setProvince_area_code(dimInfo.getString("areaCode"));
                        orderWide.setProvince_iso_code(dimInfo.getString("isoCode"));
                        orderWide.setProvince_3166_2_code(dimInfo.getString("iso31662"));
                    }

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return orderWide.getProvince_id().toString();
                    }
                }, 60, TimeUnit.SECONDS);
        //orderWideWithProvinceDS.print("orderWideWithProvinceDS>>>>>>>>>>");

        //4.3 sku
        SingleOutputStreamOperator<OrderWide> orderWideWithSkuDS = AsyncDataStream.unorderedWait(
                orderWideWithProvinceDS, new DimAsyncFunction<OrderWide>("DIM_SKU_INFO") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) throws ParseException {
                        //System.out.println("sku info: " + jsonObject);
                        orderWide.setSku_name(jsonObject.getString("skuName"));
                        orderWide.setCategory3_id(jsonObject.getLong("category3Id"));
                        orderWide.setSpu_id(jsonObject.getLong("spuId"));
                        orderWide.setTm_id(jsonObject.getLong("tmId"));
                    }

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getSku_id());
                    }
                }, 60, TimeUnit.SECONDS);

        //orderWideWithSkuDS.print("orderWideWithSkuDS>>>>>>>>>>");

        //4.4 spu
        SingleOutputStreamOperator<OrderWide> orderWideWithSpuDS = AsyncDataStream.unorderedWait(
                orderWideWithSkuDS, new DimAsyncFunction<OrderWide>("DIM_SPU_INFO") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) throws ParseException {
                        orderWide.setSpu_name(jsonObject.getString("spuName"));
                    }

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getSpu_id());
                    }
                }, 60, TimeUnit.SECONDS);
        //orderWideWithSpuDS.print("orderWideWithSpuDS>>>>>>>>>");

        //4.5 tm
        SingleOutputStreamOperator<OrderWide> orderWideWithTmDS = AsyncDataStream.unorderedWait(
                orderWideWithSpuDS, new DimAsyncFunction<OrderWide>("DIM_BASE_TRADEMARK") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) throws ParseException {
                        orderWide.setTm_name(jsonObject.getString("tmName"));
                    }

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getTm_id());
                    }
                }, 60, TimeUnit.SECONDS);
        //orderWideWithTmDS.print("orderWideWithTMDS>>>>>>>>>>>");

        //4.6 category
        SingleOutputStreamOperator<OrderWide> orderWideWithCategory3DS = AsyncDataStream.unorderedWait(
                orderWideWithTmDS, new DimAsyncFunction<OrderWide>("DIM_BASE_CATEGORY3") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) throws ParseException {
                        orderWide.setCategory3_name(jsonObject.getString("name"));
                    }

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getCategory3_id());
                    }
                }, 60, TimeUnit.SECONDS);

        orderWideWithCategory3DS.print("orderWideWithCategory3DS>>>>>>>>>>>");

        // write into kafka
        orderWideWithCategory3DS
                .map(JSONObject::toJSONString)
                .addSink(MyKafkaUtil.getKafkaProducer(orderWideSinkTopic));

        //6. execute
        env.execute("OrderWideApp");
    }
}
