package com.zz.flink.dynamic;

import com.alibaba.fastjson.JSON;
import com.zz.flink.common.model.PageView;
import com.zz.flink.common.util.TimeUtil;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Map;
import java.util.Properties;

public class DynamicAggregateTest {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
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
                .keyBy(new KeySelector<RichData, String>() {
                    @Override
                    public String getKey(RichData richData) throws Exception {
                        return richData.getKey();
                    }
                }).process(new DynamicAggregateFunction()).name("DynamicAggregateFunction").uid("DynamicAggregateFunction")
                .print();
        env.execute("dynamic-aggregate");
    }
}
