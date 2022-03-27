package com.zz.flink.streaming;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.zz.flink.common.model.PageView;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.*;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class PvStreamTest {

	public static void main(String[] args) throws Exception {
		System.out.println("test");
		final ObjectMapper mapper = new ObjectMapper();
		Configuration configuration = new Configuration();
//		configuration.setString("metrics.reporters","console_reporter");
//		configuration.setString("metrics.reporter.console_reporter.factory.class","com.zz.flink.metrics.ConsoleMetricReporterFactory");
		configuration.setString("metrics.reporters","kafka_reporter");
		configuration.setString("metrics.reporter.kafka_reporter.factory.class","com.paic.flink.metrics.KafkaMetricReporterFactory");
		configuration.setString("metrics.reporter.kafka_reporter.interval","10 SECONDS");
		configuration.setString("metrics.reporter.kafka_reporter.groupingKey","scenaioId=1;requestId=100");
		configuration.setInteger("taskmanager.cpu.cores",2);
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "localhost:9092");
// only required for Kafka 0.8
//		properties.setProperty("zookeeper.connect", "localhost:2181");
		properties.setProperty("group.id", "pvtest");
		FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>("pv",new SimpleStringSchema(),properties);
		DataStreamSource<String> stream = env.addSource(consumer);
		SingleOutputStreamOperator<PageView> pvStream = stream.map(new RichMapFunction<String, PageView>() {

			private transient Counter counter;

			private transient Meter meter;

//			private transient Histogram histogram;

			@Override
			public void open(Configuration config) throws Exception {
				MetricGroup group = getRuntimeContext().getMetricGroup();
				counter = group.counter("msgCounter");
				meter = group.meter("msgMeter",new MeterView(group.counter("msgCounterForMeter")));
			}

			@Override
			public PageView map(String s) throws Exception {
				counter.inc();
				meter.markEvent();
				return mapper.readValue(s, PageView.class);
			}
		});
		pvStream.map(new MapFunction<PageView, Tuple2<String,Integer>>() {
			@Override
			public Tuple2<String, Integer> map(PageView pageView) throws Exception {

				return new Tuple2<>(pageView.getUserId(),1);
			}
		}).keyBy(0).sum(1)
				.print().setParallelism(4);
		env.execute("kafkaTest");
	}
}
