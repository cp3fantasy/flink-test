package com.zz.flink.common.kafka;

import com.alibaba.fastjson.JSON;
import com.zz.flink.common.model.PageView;
import com.zz.flink.common.model.PvBatch;
import com.zz.flink.common.simulator.PageViewFixedSpeedSimulator;
import com.zz.flink.common.simulator.PageViewHandler;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class PageViewBatchProducer implements PageViewHandler {

    private Producer<String, String> producer;

    private AtomicLong count = new AtomicLong(0);

    private Map<String, PvBatch> batchMap = new HashMap<>();

    public PageViewBatchProducer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("delivery.timeout.ms", 5000);
        props.put("request.timeout.ms", 2000);
        props.put("batch.size", 16384);
        props.put("linger.ms", 10);
        props.put("buffer.memory", 33554432);
        producer = new KafkaProducer<>(props, new StringSerializer(), new StringSerializer());
        Executors.newSingleThreadScheduledExecutor().scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                Map<String, PvBatch> map = batchMap;
                batchMap = new HashMap<>();
                for (PvBatch batch : map.values()) {
                    send(batch);
                }
            }
        }, 10, 10, TimeUnit.SECONDS);
    }

    public void send(PvBatch batch) {
        String json = JSON.toJSONString(batch);
        ProducerRecord<String, String> record = new ProducerRecord("pv_batch", batch.getUserId(), json);
        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (exception != null) {
                    exception.printStackTrace();
                } else {
                    long cnt = count.incrementAndGet();
                    System.out.println(cnt);
                }
            }
        });
    }


    @Override
    public void handle(PageView pageView) {
        PvBatch batch = batchMap.computeIfAbsent(pageView.getUserId(), userId -> {
            PvBatch pvBatch = new PvBatch();
            pvBatch.setUserId(pageView.getUserId());
            pvBatch.setStartTime(pageView.getStartTime());
            return pvBatch;
        });
        batch.addPageView(pageView);
    }

    public static void main(String[] args) {
        PageViewBatchProducer producer = new PageViewBatchProducer();
        PageViewFixedSpeedSimulator simulator = new PageViewFixedSpeedSimulator(3, 5);
        simulator.setHandler(producer);
        simulator.start();
    }
}
