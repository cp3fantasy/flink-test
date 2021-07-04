package com.zz.flink.common.kafka;

import com.zz.flink.common.model.PageView;
import com.zz.flink.common.util.SimulateUtil;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import io.netty.util.Timer;
import io.netty.util.TimerTask;

import java.util.Random;
import java.util.concurrent.TimeUnit;

public class PageViewRandomSimulator {

    private String[] users;

    private String[] pageIds;

    private Timer timer;

    private PageViewKafkaProducer producer;

    private Random random = new Random();

    public PageViewRandomSimulator(int userCount, int pageCount) {
        users = SimulateUtil.createStringArray("user", userCount);
        pageIds = SimulateUtil.createStringArray("page", pageCount);
        timer = new HashedWheelTimer();
    }

    public void setProducer(PageViewKafkaProducer producer) {
        this.producer = producer;
    }

    public void start() {
        for (String user : users) {
            browse(user,0);
        }
    }

    private void browse(final String user,int delay) {
        timer.newTimeout(new TimerTask() {
            @Override
            public void run(Timeout timeout) throws Exception {
                PageView pageView = new PageView();
                pageView.setUserId(user);
                pageView.setPageId(pageIds[random.nextInt(pageIds.length)]);
                pageView.setStartTime(System.currentTimeMillis());
                pageView.setEndTime(pageView.getStartTime()+new Random().nextInt(5000));
                System.out.println(pageView);
                producer.send(pageView);
                browse(user,random.nextInt(5000));
            }
        }, delay, TimeUnit.MILLISECONDS);
    }

    public static void main(String[] args) {
        PageViewKafkaProducer producer = new PageViewKafkaProducer();
        PageViewRandomSimulator simulator = new PageViewRandomSimulator(1,10);
        simulator.setProducer(producer);
        simulator.start();
    }
}
