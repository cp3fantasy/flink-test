package com.zz.flink.dynamic;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
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
        ParameterTool parameters = ParameterTool.fromArgs(args);
        Configuration config = new Configuration();
        config.setInteger(RestOptions.PORT, 7100);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);
        env.getConfig().setGlobalJobParameters(parameters);
        env.enableCheckpointing(30000);
        String configUrl = parameters.get("configUrl","localhost:8080");
        RuleManager.init(configUrl);
        Properties properties = new Properties();
        String servers = parameters.get("servers","localhost:9092");
        String topic = parameters.get("topic");
        properties.setProperty("bootstrap.servers", servers);
        properties.setProperty("group.id", "dynamic-aggregator");
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), properties);
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
