package com.zz.flink.streaming.connector;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class KafkaTest {

	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "localhost:9092");
// only required for Kafka 0.8
//		properties.setProperty("zookeeper.connect", "localhost:2181");
//		properties.setProperty("group.id", "test");
		FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>("test",new SimpleStringSchema(),properties);
		DataStreamSource<String> stream = env.addSource(consumer);
		stream.print().setParallelism(1);
		env.execute("kafkaTest");
	}
}
