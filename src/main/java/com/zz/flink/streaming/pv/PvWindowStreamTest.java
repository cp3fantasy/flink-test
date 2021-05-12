package com.zz.flink.streaming.pv;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.zz.flink.common.model.PageView;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class PvWindowStreamTest {

	public static void main(String[] args) throws Exception {
		final ObjectMapper mapper = new ObjectMapper();
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
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
		});
//		pvStream.assignTimestampsAndWatermarks()
		pvStream.map(new MapFunction<PageView, Tuple2<String,Integer>>() {
			@Override
			public Tuple2<String, Integer> map(PageView pageView) throws Exception {
				return new Tuple2<>(pageView.getPageId(),1);
			}
		}).keyBy(0).window(TumblingProcessingTimeWindows
				.of(Time.seconds(10)))
				.trigger(new Trigger<Tuple2<String, Integer>, TimeWindow>() {
					@Override
					public TriggerResult onElement(Tuple2<String, Integer> element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
						return TriggerResult.FIRE;
					}

					@Override
					public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
						return TriggerResult.CONTINUE;
					}

					@Override
					public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
						return TriggerResult.CONTINUE;
					}

					@Override
					public void clear(TimeWindow window, TriggerContext ctx) throws Exception {

					}
				})
				.sum(1).print().setParallelism(1);
		env.execute("kafkaTest");
	}
}
