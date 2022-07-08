package com.luban.micromall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.luban.micromall.realtime.app.function.DimAsyncFunction;
import com.luban.micromall.realtime.bean.OrderWide;
import com.luban.micromall.realtime.bean.PaymentWide;
import com.luban.micromall.realtime.bean.ProductStats;
import com.luban.micromall.realtime.common.MicromallConstants;
import com.luban.micromall.realtime.utils.ClickHouseUtil;
import com.luban.micromall.realtime.utils.DateTimeUtil;
import com.luban.micromall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.time.Duration;
import java.util.Date;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

public class ProductStatsApp {
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

        //2. read 7 topics from kafka
        String groupId = "product_stats_app";

        String pageViewSourceTopic = "dwd_page_log";
        String orderWideSourceTopic = "dwm_order_wide";
        String paymentWideSourceTopic = "dwm_payment_wide";
        String cartInfoSourceTopic = "dwd_cart_info";
        String favorInfoSourceTopic = "dwd_favor_info";
        String refundInfoSourceTopic = "dwd_order_refund_info";
        String commentInfoSourceTopic = "dwd_comment_info";
        DataStreamSource<String> pvDS = env.addSource(MyKafkaUtil.getKafkaConsumer(pageViewSourceTopic, groupId));
        DataStreamSource<String> favorDS = env.addSource(MyKafkaUtil.getKafkaConsumer(favorInfoSourceTopic, groupId));
        DataStreamSource<String> cartDS = env.addSource(MyKafkaUtil.getKafkaConsumer(cartInfoSourceTopic, groupId));
        DataStreamSource<String> orderDS = env.addSource(MyKafkaUtil.getKafkaConsumer(orderWideSourceTopic, groupId));
        DataStreamSource<String> payDS = env.addSource(MyKafkaUtil.getKafkaConsumer(paymentWideSourceTopic, groupId));
        DataStreamSource<String> refundDS = env.addSource(MyKafkaUtil.getKafkaConsumer(refundInfoSourceTopic, groupId));
        DataStreamSource<String> commentDS = env.addSource(MyKafkaUtil.getKafkaConsumer(commentInfoSourceTopic, groupId));

        //3. convert the 7 streams to the same ProductStats type
        SingleOutputStreamOperator<ProductStats> productStatsWithClickAndDisplayDS = pvDS.flatMap(new FlatMapFunction<String, ProductStats>() {
            @Override
            public void flatMap(String s, Collector<ProductStats> collector) throws Exception {
                JSONObject jsonObject = JSON.parseObject(s);
                JSONObject page = jsonObject.getJSONObject("page");
                String pageId = page.getString("page_id");
                Long ts = jsonObject.getLong("ts");
                if ("good_detail".equals(pageId) && "sku_id".equals(page.getString("item_type"))) {
                    collector.collect(ProductStats.builder()
                            .sku_id(page.getLong("item"))
                            .click_ct(1L)
                            .ts(ts)
                            .build());
                }

                JSONArray displays = jsonObject.getJSONArray("displays");
                if (displays != null && displays.size() > 0) {
                    for (int i = 0; i < displays.size(); i++) {
                        JSONObject display = displays.getJSONObject(i);
                        if ("sku_id".equals(display.getString("item_type"))) {
                            collector.collect(ProductStats.builder()
                                    .sku_id(display.getLong("item"))
                                    .display_ct(1L)
                                    .ts(ts)
                                    .build());
                        }
                    }
                }
            }
        });

        SingleOutputStreamOperator<ProductStats> productStatsWithFavorDS = favorDS.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);
            //System.out.println("jsonObj: " + jsonObject);
            return ProductStats.builder()
                    .sku_id(jsonObject.getLong("sku_id"))
                    .favor_ct(1L)
                    //.ts(DateTimeUtil.toTs(jsonObject.getString("create_time")))
                    .ts(jsonObject.getLong("create_time"))
                    .build();
        });

        SingleOutputStreamOperator<ProductStats> productStatsWithCartDS = cartDS.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);
            return ProductStats.builder()
                    .sku_id(jsonObject.getLong("sku_id"))
                    .cart_ct(1L)
                    //.ts(DateTimeUtil.toTs(jsonObject.getString("create_time")))
                    .ts(jsonObject.getLong("create_time"))
                    .build();
        });

        SingleOutputStreamOperator<ProductStats> productStatsWithOrderDS = orderDS.map(line -> {
            OrderWide orderWide = JSON.parseObject(line, OrderWide.class);
            HashSet<Long> orderIds = new HashSet<Long>();
            orderIds.add(orderWide.getOrder_id());
            return ProductStats.builder()
                    .sku_id(orderWide.getSku_id())
                    .order_sku_num(orderWide.getSku_num())
                    .order_amount(orderWide.getOrder_price())
                    .orderIdSet(orderIds)
                    //.ts(DateTimeUtil.toTs(orderWide.getCreate_time()))
                    .ts(new Long(orderWide.getCreate_time()))
                    .build();
        });

        SingleOutputStreamOperator<ProductStats> productStatsWithPaymentDS = payDS.map(line -> {
            PaymentWide paymentWide = JSON.parseObject(line, PaymentWide.class);
            HashSet<Long> orderIds = new HashSet<>();
            orderIds.add(paymentWide.getOrder_id());
            return ProductStats.builder()
                    .sku_id(paymentWide.getSku_id())
                    .payment_amount(paymentWide.getOrder_price())
                    .paidOrderIdSet(orderIds)
                    //.ts(DateTimeUtil.toTs(paymentWide.getPayment_create_time()))
                    .ts(new Long(paymentWide.getPayment_create_time()))
                    .build();
        });

        SingleOutputStreamOperator<ProductStats> productStatsWithRefundDS = refundDS.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);
            HashSet<Long> orderIds = new HashSet<>();
            orderIds.add(jsonObject.getLong("order_id"));
            return ProductStats.builder()
                    .sku_id(jsonObject.getLong("sku_id"))
                    .refund_amount(jsonObject.getBigDecimal("refund_amount"))
                    .refundOrderIdSet(orderIds)
                    //.ts(DateTimeUtil.toTs(jsonObject.getString("create_time")))
                    .ts(jsonObject.getLong("create_time"))
                    .build();
        });

        SingleOutputStreamOperator<ProductStats> productStatsWithCommentDS = commentDS.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);
            String appraise = jsonObject.getString("appraise");
            long goodCt = 0L;
            if (MicromallConstants.APPRAISE_GOOD.equals(appraise)) {
                goodCt = 1L;
            }
            return ProductStats.builder()
                    .sku_id(jsonObject.getLong("sku_id"))
                    //.ts(DateTimeUtil.toTs(jsonObject.getString("create_time")))
                    .ts(jsonObject.getLong("create_time"))
                    .comment_ct(1L)
                    .good_comment_ct(goodCt)
                    .build();
        });

        //4. union the 7 streams
        DataStream<ProductStats> unionDS = productStatsWithClickAndDisplayDS.union(
                productStatsWithFavorDS,
                productStatsWithCartDS,
                productStatsWithOrderDS,
                productStatsWithPaymentDS,
                productStatsWithRefundDS,
                productStatsWithCommentDS
        );

        //5. assign timestamp and watermark
        SingleOutputStreamOperator<ProductStats> productStatsWithWMDS = unionDS.assignTimestampsAndWatermarks(WatermarkStrategy
                .<ProductStats>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<ProductStats>() {
                    @Override
                    public long extractTimestamp(ProductStats productStats, long l) {
                        return productStats.getTs();
                    }
                }));

        //6. keyBy, window, aggregate
        SingleOutputStreamOperator<ProductStats> reduceDS = productStatsWithWMDS.keyBy(ProductStats::getSku_id)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<ProductStats>() {
                    @Override
                    public ProductStats reduce(ProductStats stats1, ProductStats stats2) throws Exception {
                        stats1.setDisplay_ct(stats1.getDisplay_ct() + stats2.getDisplay_ct());
                        stats1.setClick_ct(stats1.getClick_ct() + stats2.getClick_ct());
                        stats1.setCart_ct(stats1.getCart_ct() + stats2.getCart_ct());
                        stats1.setFavor_ct(stats1.getFavor_ct() + stats2.getFavor_ct());
                        stats1.setOrder_amount(stats1.getOrder_amount().add(stats2.getOrder_amount()));
                        stats1.getOrderIdSet().addAll(stats2.getOrderIdSet());
                        //stats1.setOrder_ct(stats1.getOrderIdSet().size() + 0L);
                        stats1.setOrder_sku_num(stats1.getOrder_sku_num() + stats2.getOrder_sku_num());
                        stats1.setPayment_amount(stats1.getPayment_amount().add(stats2.getPayment_amount()));

                        stats1.getRefundOrderIdSet().addAll(stats2.getRefundOrderIdSet());
                        //stats1.setRefund_order_ct(stats1.getRefundOrderIdSet().size() + 0L);
                        stats1.setRefund_amount(stats1.getRefund_amount().add(stats2.getRefund_amount()));

                        stats1.getPaidOrderIdSet().addAll(stats2.getPaidOrderIdSet());
                        //stats1.setPaid_order_ct(stats1.getPaidOrderIdSet().size() + 0L);

                        stats1.setComment_ct(stats1.getComment_ct() + stats2.getComment_ct());
                        stats1.setGood_comment_ct(stats1.getGood_comment_ct() + stats2.getGood_comment_ct());
                        return stats1;
                    }
                }, new WindowFunction<ProductStats, ProductStats, Long, TimeWindow>() {
                    @Override
                    public void apply(Long aLong, TimeWindow timeWindow, Iterable<ProductStats> iterable, Collector<ProductStats> collector) throws Exception {
                        ProductStats productStats = iterable.iterator().next();

                        productStats.setStt(DateTimeUtil.toYMDhms(new Date(timeWindow.getStart())));
                        productStats.setEdt(DateTimeUtil.toYMDhms(new Date(timeWindow.getEnd())));

                        productStats.setOrder_ct((long) productStats.getOrderIdSet().size());
                        productStats.setPaid_order_ct((long) productStats.getPaidOrderIdSet().size());
                        productStats.setRefund_order_ct((long) productStats.getRefundOrderIdSet().size());

                        collector.collect(productStats);
                    }
                });

        //7. join with dimensions
        //7.1 sku
        SingleOutputStreamOperator<ProductStats> productStatsWithSkuDS = AsyncDataStream.unorderedWait(reduceDS,
                new DimAsyncFunction<ProductStats>("DIM_SKU_INFO") {
                    @Override
                    public void join(ProductStats productStats, JSONObject dimInfo) throws ParseException {
                        productStats.setSku_name(dimInfo.getString("skuName"));
                        productStats.setSku_price(dimInfo.getBigDecimal("price"));
                        productStats.setSpu_id(dimInfo.getLong("spuId"));
                        productStats.setTm_id(dimInfo.getLong("tmId"));
                        productStats.setCategory3_id(dimInfo.getLong("category3Id"));
                    }

                    @Override
                    public String getKey(ProductStats productStats) {
                        return productStats.getSku_id().toString();
                    }
                }, 60, TimeUnit.SECONDS);

        //7.2 spu
        SingleOutputStreamOperator<ProductStats> productStatsWithSpuDS =
                AsyncDataStream.unorderedWait(productStatsWithSkuDS,
                        new DimAsyncFunction<ProductStats>("DIM_SPU_INFO") {
                            @Override
                            public void join(ProductStats productStats, JSONObject jsonObject) throws ParseException {
                                productStats.setSpu_name(jsonObject.getString("spuName"));
                            }

                            @Override
                            public String getKey(ProductStats productStats) {
                                return String.valueOf(productStats.getSpu_id());
                            }
                        }, 60, TimeUnit.SECONDS);

        //7.3 category
        SingleOutputStreamOperator<ProductStats> productStatsWithCategory3DS =
                AsyncDataStream.unorderedWait(productStatsWithSpuDS,
                        new DimAsyncFunction<ProductStats>("DIM_BASE_CATEGORY3") {
                            @Override
                            public void join(ProductStats productStats, JSONObject jsonObject) throws ParseException {
                                productStats.setCategory3_name(jsonObject.getString("name"));
                            }

                            @Override
                            public String getKey(ProductStats productStats) {
                                return String.valueOf(productStats.getCategory3_id());
                            }
                        }, 60, TimeUnit.SECONDS);

        //7.4 tm
        SingleOutputStreamOperator<ProductStats> productStatsWithTmDS =
                AsyncDataStream.unorderedWait(productStatsWithCategory3DS,
                        new DimAsyncFunction<ProductStats>("DIM_BASE_TRADEMARK") {
                            @Override
                            public void join(ProductStats productStats, JSONObject jsonObject) throws ParseException {
                                productStats.setTm_name(jsonObject.getString("tmName"));
                            }

                            @Override
                            public String getKey(ProductStats productStats) {
                                return String.valueOf(productStats.getTm_id());
                            }
                        }, 60, TimeUnit.SECONDS);

        //8. write into clickhouse
        productStatsWithTmDS.print();
        productStatsWithTmDS.addSink(ClickHouseUtil.getSink("insert into table product_stats values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"));

        //9. execute
        env.execute("ProductStatsApp");
    }
}
