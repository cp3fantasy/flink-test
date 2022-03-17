package com.zz.flink.metrics;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class FlinkMetricsToPrometheus {

	public static void main(String[] args) throws Exception {
		final ObjectMapper mapper = new ObjectMapper();
		Configuration configuration = new Configuration();
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "localhost:9092");
// only required for Kafka 0.8
//		properties.setProperty("zookeeper.connect", "localhost:2181");
		properties.setProperty("group.id", "flink-metrics-consumer");
		FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>("flink-metrics",new SimpleStringSchema(),properties);
		DataStreamSource<String> stream = env.addSource(consumer);
		stream.print();
//		SingleOutputStreamOperator<PageView> pvStream = stream.map(new RichMapFunction<String, PageView>() {
//
//			private transient Counter counter;
//
//			private transient Meter meter;
//
////			private transient Histogram histogram;
//
//			@Override
//			public void open(Configuration config) throws Exception {
//				MetricGroup group = getRuntimeContext().getMetricGroup();
//				counter = group.counter("msgCounter");
//				meter = group.meter("msgMeter",new MeterView(group.counter("msgCounterForMeter")));
//			}
//
//			@Override
//			public PageView map(String s) throws Exception {
//				counter.inc();
//				meter.markEvent();
//				return mapper.readValue(s, PageView.class);
//			}
//		});
//		pvStream.map(new MapFunction<PageView, Tuple2<String,Integer>>() {
//			@Override
//			public Tuple2<String, Integer> map(PageView pageView) throws Exception {
//
//				return new Tuple2<>(pageView.getUserId(),1);
//			}
//		}).keyBy(0).sum(1)
//				.print().setParallelism(1);
//		env.execute("kafkaTest");
	}
}
