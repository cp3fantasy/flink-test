package com.zz.flink.common.kafka;

import com.zz.flink.common.model.PageView;
import com.zz.flink.common.simulator.PageViewHandler;
import org.apache.kafka.clients.producer.*;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

public class PageViewKafkaProducer implements PageViewHandler {

    private Producer<String, PageView> producer;

    private AtomicLong count = new AtomicLong(0);

    public PageViewKafkaProducer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("delivery.timeout.ms",5000);
        props.put("request.timeout.ms", 2000);
        props.put("batch.size", 16384);
        props.put("linger.ms", 10);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", PageViewSerializer.class);
        producer = new KafkaProducer<>(props);
    }

    public void send(PageView pageView) {
        ProducerRecord<String, PageView> record = new ProducerRecord("pv", pageView.getUserId(), pageView);
        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (exception != null) {
                    exception.printStackTrace();
                } else {
                    long cnt = count.incrementAndGet();
                    if (cnt % 10 == 0) {
                        System.out.println(cnt);
                    }
                }
            }
        });
    }


    @Override
    public void handle(PageView pageView) {
        send(pageView);
    }
}
