package com.zz.flink.common.kafka;

import com.zz.flink.common.model.PageView;
import com.zz.flink.common.util.SimulateUtil;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import io.netty.util.Timer;
import io.netty.util.TimerTask;

import java.util.Random;
import java.util.concurrent.TimeUnit;

public class PageViewFixedSpeedSimulator {

    private String[] users;

    private String[] pageIds;

    private Timer timer;

    private PageViewKafkaProducer producer;

    public PageViewFixedSpeedSimulator(int userCount, int pageCount) {
        users = SimulateUtil.createStringArray("user", userCount);
        pageIds = SimulateUtil.createStringArray("page", pageCount);
        timer = new HashedWheelTimer();
    }

    public void setProducer(PageViewKafkaProducer producer) {
        this.producer = producer;
    }

    public void start() {
        for (String user : users) {
            TimerTask task = new TimerTask() {

                int count = 0;

                @Override
                public void run(Timeout timeout) throws Exception {
                    PageView pageView = new PageView();
                    pageView.setUserId(user);
                    pageView.setPageId(pageIds[count % pageIds.length]);
                    count++;
                    pageView.setStartTime(System.currentTimeMillis());
                    pageView.setEndTime(pageView.getStartTime()+new Random().nextInt(3000));
                    System.out.println(pageView);
                    producer.send(pageView);
                    timer.newTimeout(this, 3000, TimeUnit.MILLISECONDS);
                }
            };
            timer.newTimeout(task, 1000, TimeUnit.MILLISECONDS);
        }
    }


    public static void main(String[] args) {
        PageViewKafkaProducer producer = new PageViewKafkaProducer();
        PageViewFixedSpeedSimulator simulator = new PageViewFixedSpeedSimulator(5, 10);
        simulator.setProducer(producer);
        simulator.start();
    }
}
