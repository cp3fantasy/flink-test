package com.zz.flink.streaming;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.influxdb.dto.Point;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class FlinkMetricsToInflux {


    public static void main(String[] args) throws Exception {
        final ObjectMapper mapper = new ObjectMapper();
        Configuration configuration = new Configuration();
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
// only required for Kafka 0.8
//		properties.setProperty("zookeeper.connect", "localhost:2181");
        properties.setProperty("group.id", "flink-metrics-consumer");
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>("flink_metrics", new SimpleStringSchema(), properties);
        DataStreamSource<String> stream = env.addSource(consumer);
        SingleOutputStreamOperator<String> influxStream = stream.map(new RichMapFunction<String, MetricInfo>() {
            @Override
            public MetricInfo map(String value) throws Exception {
                return mapper.readValue(value, MetricInfo.class);
            }
        }).filter(new FilterFunction<MetricInfo>() {
            @Override
            public boolean filter(MetricInfo info) throws Exception {
                return info.getValue() != null;
            }
        }).map(new MapFunction<MetricInfo, String>() {

            SimpleDateFormat timeFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

            private long getMillis(String time) {
                try {
                    return timeFormat.parse(time).getTime();
                } catch (ParseException e) {
                    e.printStackTrace();
                }
                return System.currentTimeMillis();
            }

            @Override
            public String map(MetricInfo info) throws Exception {
                Point.Builder builder =
                        Point.measurement(info.getName())
                                .tag(info.getTags())
                                .time(getMillis(info.getTime()), TimeUnit.SECONDS);
                Object value = info.getValue();
                if (value instanceof String) {
                    builder.addField("value", (String) value);
                } else if (value instanceof Boolean) {
                    builder.addField("value", (Boolean) value);
                } else if (value instanceof Integer) {
                    builder.addField("value", (Integer) value);
                } else if (value instanceof Long) {
                    builder.addField("value", (Long) value);
                } else if (value instanceof Double) {
                    builder.addField("value", (Double) value);
                } else {
                    Map<String, Object> map = (Map<String, Object>) value;
                    for (Map.Entry<String, Object> entry : map.entrySet()) {
                        Object hValue = entry.getValue();
                        if (hValue instanceof Integer) {
                            builder.addField(entry.getKey(), (Integer) hValue);
                        } else if (hValue instanceof Long) {
                            builder.addField(entry.getKey(), (Long) hValue);
                        } else if (hValue instanceof Double) {
                            builder.addField(entry.getKey(), (Double) hValue);
                        }
                    }
                }
                return builder.build().lineProtocol(TimeUnit.SECONDS);
            }
        });
        influxStream.print();
        Properties producerProperties = new Properties();
        producerProperties.setProperty("bootstrap.servers", "localhost:9092");
        FlinkKafkaProducer<String> myProducer = new FlinkKafkaProducer<>(
                "metrics",
                new SimpleStringSchema(),
                producerProperties);
        influxStream.addSink(myProducer);
        env.execute("metricsToInflux");

    }

}
