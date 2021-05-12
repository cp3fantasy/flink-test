package com.zz.flink.streaming.pv;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.zz.flink.common.model.PageView;
import com.zz.flink.common.util.TimeUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
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

import java.util.Properties;

public class PvProcessFunctionTest {

    public static void main(String[] args) throws Exception {
        final ObjectMapper mapper = new ObjectMapper();
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
// only required for Kafka 0.8
//		properties.setProperty("zookeeper.connect", "localhost:2181");
        properties.setProperty("group.id", "pvtest");
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>("pv", new SimpleStringSchema(), properties);
        DataStreamSource<String> stream = env.addSource(consumer);
        SingleOutputStreamOperator<PageView> pvStream = stream.map(new MapFunction<String, PageView>() {
            @Override
            public PageView map(String s) throws Exception {
                return mapper.readValue(s, PageView.class);
            }
        });
//		pvStream.map(new MapFunction<PageView, Tuple2<String,Integer>>() {
//			@Override
//			public Tuple2<String, Integer> map(PageView pageView) throws Exception {
//				return new Tuple2<>(pageView.getUserId(),1);
//			}
//		}).keyBy(0).sum(1)
//				.print().setParallelism(1);
        KeyedStream<PageView, Tuple> userPvStream = pvStream.keyBy("userId");
        userPvStream.process(new KeyedProcessFunction<Tuple, PageView, Tuple3<String,String, Integer>>() {

            private ValueState<Integer> countState;

            private ValueState<Long> windowState;

//            @Override
//            public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple3<String,String, Integer>> out) throws Exception {
//                super.onTimer(timestamp, ctx, out);
//            }

            @Override
            public void open(Configuration parameters) throws Exception {
                countState = getRuntimeContext().getState(new ValueStateDescriptor<>("pv", Integer.class));
                windowState = getRuntimeContext().getState(new ValueStateDescriptor<>("window", Long.class));
            }


            @Override
            public void processElement(PageView pageView, Context ctx, Collector<Tuple3<String, String, Integer>> out) throws Exception {
                long windowTime = pageView.getStartTime() / 5000 * 5000;
                Long window = windowState.value();
                if (window == null) {
                    windowState.update(windowTime);
                } else if (windowTime != window) {
                    countState.clear();
                    windowState.update(windowTime);
                }
                Integer count = countState.value();
                if (count == null) {
                    count = 1;
                } else {
                    count++;
                }
                countState.update(count);
                out.collect(new Tuple3<>(TimeUtil.format(windowTime), ctx.getCurrentKey().toString(), count));
            }

        }).print().setParallelism(1);
        env.execute("kafkaTest");
    }
}
