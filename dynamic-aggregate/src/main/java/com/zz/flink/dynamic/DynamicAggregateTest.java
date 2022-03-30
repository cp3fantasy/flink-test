package com.zz.flink.dynamic;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.time.Duration;
import java.util.Map;
import java.util.Properties;

public class DynamicAggregateTest {

    public static void main(String[] args) throws Exception {
        Configuration config = new Configuration();
        config.setInteger(RestOptions.PORT, 7100);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);
        env.enableCheckpointing(30000);
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
// only required for Kafka 0.8
//		properties.setProperty("zookeeper.connect", "localhost:2181");
        properties.setProperty("group.id", "pvtest");

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>("pv", new SimpleStringSchema(), properties);
        DataStreamSource<String> stream = env.addSource(consumer);
        stream.map(new MapFunction<String, Map<String, Object>>() {
            @Override
            public Map<String, Object> map(String s) throws Exception {
                return (Map<String, Object>) JSON.parse(s);
            }
        }).flatMap(new DynamicMapFunction()).name("DynamicMapFunction").uid("DynamicMapFunction")
                .assignTimestampsAndWatermarks(WatermarkStrategy.<RichData>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((data, recordTimestamp) -> data.getEventTime()))
                .keyBy((KeySelector<RichData, String>) richData -> richData.getKey())
                .process(new DynamicWindowAggregateFunction()).name("DynamicWindowAggregateFunction").uid("DynamicWindowAggregateFunction")
                .print();
        env.execute("dynamic-aggregate");
    }
}
