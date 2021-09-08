package com.zz.flink.debezium;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

public class CdcMessageProducer {

    public static void main(String[] args) throws InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("delivery.timeout.ms", 5000);
        props.put("request.timeout.ms", 2000);
        props.put("batch.size", 16384);
        props.put("linger.ms", 10);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        Producer<String, String> producer = new KafkaProducer<>(props);
        for (int i = 0; i < 100; i++) {
            ProducerRecord<String, String> record = new ProducerRecord("products_binlog", buildCdcMsg(i));
            final int j = i;
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception != null) {
                        exception.printStackTrace();
                    } else {
                        System.out.println(j);
                    }
                }
            });
            Thread.sleep(1000);
        }
    }

    private static String buildCdcMsg(int i) {
        String format = "{\n" +
                "  \"before\": {\n" +
                "    \"id\": %d,\n" +
                "    \"name\": \"scooter\",\n" +
                "    \"description\": \"Big 2-wheel scooter\",\n" +
                "    \"weight\": 5.18\n" +
                "  },\n" +
                "  \"after\": {\n" +
                "    \"id\": %d,\n" +
                "    \"name\": \"scooter\",\n" +
                "    \"description\": \"Big 2-wheel scooter\",\n" +
                "    \"weight\": 5.15\n" +
                "  },\n" +
                "  \"source\": {},\n" +
                "  \"op\": \"u\",\n" +
                "  \"ts_ms\": %d,\n" +
                "  \"transaction\": null\n" +
                "}";
        return String.format(format, i, i, System.currentTimeMillis());
    }


}
