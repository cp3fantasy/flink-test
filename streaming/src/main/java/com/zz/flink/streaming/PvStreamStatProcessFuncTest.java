package com.zz.flink.streaming;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.zz.flink.common.model.PageView;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.time.Duration;
import java.util.Properties;

public class PvStreamStatProcessFuncTest {

	public static void main(String[] args) throws Exception {
		final ObjectMapper mapper = new ObjectMapper();
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "localhost:9092");
// only required for Kafka 0.8
//		properties.setProperty("zookeeper.connect", "localhost:2181");
		properties.setProperty("group.id", "pvtest");
		FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>("pv",new SimpleStringSchema(),properties);
		DataStreamSource<String> stream = env.addSource(consumer);
		SingleOutputStreamOperator<PageView> pvStream = stream.map(new MapFunction<String, PageView>() {
			@Override
			public PageView map(String s) throws Exception {
				return mapper.readValue(s, PageView.class);
			}
		}).assignTimestampsAndWatermarks(WatermarkStrategy.<PageView>forBoundedOutOfOrderness(Duration.ofSeconds(20))
				.withTimestampAssigner(new SerializableTimestampAssigner<PageView>() {
					@Override
					public long extractTimestamp(PageView pageView, long l) {
						return pageView.getStartTime();
					}
				}));
		pvStream.map(new MapFunction<PageView, Tuple2<String,Integer>>() {
			@Override
			public Tuple2<String, Integer> map(PageView pageView) throws Exception {
				return new Tuple2<>(pageView.getUserId(),1);
			}
		}).keyBy(0).sum(1)
				.print().setParallelism(1);
		env.execute("kafkaTest");
	}
}
