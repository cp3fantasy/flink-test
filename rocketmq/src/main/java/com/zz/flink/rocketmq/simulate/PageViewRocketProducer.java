package com.zz.flink.rocketmq.simulate;

import com.alibaba.fastjson.JSON;
import com.zz.flink.common.model.PageView;
import com.zz.flink.common.simulator.PageViewHandler;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

import java.util.concurrent.atomic.AtomicLong;

public class PageViewRocketProducer implements PageViewHandler {

    private DefaultMQProducer producer;

    // Producer config
    private static final String NAME_SERVER_ADDR = "localhost:9876";
    private static final String PRODUCER_GROUP = "pv_producer";
    private static final String TOPIC = "pv";

    private AtomicLong count = new AtomicLong(0);

    public PageViewRocketProducer() {
        producer =
                new DefaultMQProducer(PRODUCER_GROUP);
        producer.setNamesrvAddr(NAME_SERVER_ADDR);
        try {
            producer.start();
        } catch (MQClientException e) {
            e.printStackTrace();
        }
    }

    public void send(PageView pageView) {
        Message msg = new Message(TOPIC, JSON.toJSONBytes(pageView));
        try {
            SendResult sendResult = producer.send(msg);
            assert sendResult != null;
            System.out.printf(
                    "send result: %s %s\n",
                    sendResult.getMsgId(), sendResult.getMessageQueue().toString());
            Thread.sleep(1000);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private String toString(PageView pageView) {
        return null;
    }


    @Override
    public void handle(PageView pageView) {
        send(pageView);
    }
}
