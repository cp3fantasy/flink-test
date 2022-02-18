package com.zz.flink.streaming;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.zz.flink.common.model.PageView;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class PvStreamStatTest {

    public static void main(String[] args) throws Exception {
        final ObjectMapper mapper = new ObjectMapper();
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "pvtest");
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>("pv", new SimpleStringSchema(), properties);
        DataStreamSource<String> stream = env.addSource(consumer);
        SingleOutputStreamOperator<PageView> pvStream = stream.map(new MapFunction<String, PageView>() {
            @Override
            public PageView map(String s) throws Exception {
                return mapper.readValue(s, PageView.class);
            }
        });
        pvStream.process(new ProcessFunction<PageView, Tuple2<String, Integer>>() {

            private Map<String, Integer> localMap;


            @Override
            public void open(Configuration parameters) throws Exception {
                localMap = new HashMap<>();
            }

            @Override
            public void processElement(PageView pageView, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                Integer count = localMap.get(pageView.getUserId());
                count = count == null ? 1 : count + 1;
                if (count == 3) {
                    out.collect(new Tuple2<>(pageView.getUserId(), count));
                    localMap.remove(pageView.getUserId());
                } else {
                    localMap.put(pageView.getUserId(), count);
                }
            }
        }).print();
        env.execute("kafkaTest");
    }
}
