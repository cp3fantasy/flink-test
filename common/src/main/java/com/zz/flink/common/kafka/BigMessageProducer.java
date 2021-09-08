package com.zz.flink.common.kafka;

import com.alibaba.fastjson.JSON;
import com.zz.flink.common.model.PageView;
import com.zz.flink.common.util.SimulateUtil;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import io.netty.util.Timer;
import io.netty.util.TimerTask;
import org.apache.kafka.clients.producer.*;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class BigMessageProducer {

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
            ProducerRecord<String, String> record = new ProducerRecord("big", buildBigMsg(2000));
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

    private static String buildBigMsg(int fieldCount) {
        Map<String, String> map = new HashMap<>();
        for (int i = 0; i < fieldCount; i++) {
            map.put("longlonglongfield" + i, "v" + i);
        }
        return JSON.toJSONString(map);
    }
}
