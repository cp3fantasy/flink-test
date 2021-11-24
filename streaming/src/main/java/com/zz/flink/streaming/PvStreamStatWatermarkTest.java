package com.zz.flink.streaming;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.zz.flink.common.util.TimeUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class PvStreamStatWatermarkTest {

    private static final TypeReference<Map<String, Object>> MAP_TYPE_REFERENCE =
            new TypeReference<Map<String, Object>>() {
            };

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
// only required for Kafka 0.8
//		properties.setProperty("zookeeper.connect", "localhost:2181");
        properties.setProperty("group.id", "pvtest");
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>("pv_stat_15s", new SimpleStringSchema(), properties);
        DataStreamSource<String> stream = env.addSource(consumer);
        SingleOutputStreamOperator<Map<String, Object>> pvStream = stream.map(new MapFunction<String, Map<String, Object>>() {
            @Override
            public Map<String, Object> map(String s) throws Exception {
                return JSON.parseObject(s, MAP_TYPE_REFERENCE);
            }
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<Map<String, Object>>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner(new SerializableTimestampAssigner<Map<String, Object>>() {
                    @Override
                    public long extractTimestamp(Map<String, Object> map, long l) {
                        String timeStr = (String) map.get("ts");
                        return TimeUtil.toMillis(timeStr);
                    }
                }));
        pvStream.keyBy(new KeySelector<Map<String, Object>, String>() {
            @Override
            public String getKey(Map<String, Object> map) throws Exception {
                return (String) map.get("userId");
            }
        })
                .window(TumblingEventTimeWindows.of(Time.seconds(15)))
                .reduce(new ReduceFunction<Map<String, Object>>() {
                    @Override
                    public Map<String, Object> reduce(Map<String, Object> m1, Map<String, Object> m2) throws Exception {
                        Map<String, Object> m = new HashMap<>();
                        m.put("userId", m1.get("userId"));
                        Integer cnt1 = (Integer) m1.get("cnt");
                        Integer cnt2 = (Integer) m2.get("cnt");
                        m.put("cnt", cnt1 + cnt2);
                        m.put("window", m1.get("ts"));
                        m.put("now", TimeUtil.format(System.currentTimeMillis()));
                        return m;
                    }
                }).print();
        env.execute("kafkaTest");
    }
}
