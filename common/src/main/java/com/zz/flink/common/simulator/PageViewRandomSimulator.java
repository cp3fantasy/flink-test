package com.zz.flink.common.simulator;

import com.zz.flink.common.kafka.PageViewKafkaProducer;
import com.zz.flink.common.model.PageView;
import com.zz.flink.common.util.SimulateUtil;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import io.netty.util.Timer;
import io.netty.util.TimerTask;

import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class PageViewRandomSimulator {

    private AtomicInteger seqNo = new AtomicInteger();

    private String[] users;

    private String[] pageIds;

    private int delay;

    private Timer timer;

    private PageViewHandler handler;

    private Random random = new Random();

    public PageViewRandomSimulator(int userCount, int pageCount, int delay) {
        users = SimulateUtil.createStringArray("user", userCount);
        pageIds = SimulateUtil.createStringArray("page", pageCount);
        timer = new HashedWheelTimer();
        this.delay = delay;
    }

    public void setHandler(PageViewHandler handler) {
        this.handler = handler;
    }

    public void start() {
        for (String user : users) {
            browse(user, 0);
        }
    }

    private void browse(final String user, int delay) {
        timer.newTimeout(new TimerTask() {
            @Override
            public void run(Timeout timeout) throws Exception {
                PageView pageView = new PageView();
                pageView.setSeqNo(String.valueOf(seqNo.incrementAndGet()));
                pageView.setUserId(user);
                pageView.setPageId(pageIds[random.nextInt(pageIds.length)]);
                pageView.setStartTime(System.currentTimeMillis());
                pageView.setEndTime(pageView.getStartTime() + new Random().nextInt(PageViewRandomSimulator.this.delay));
                System.out.println(pageView);
                handler.handle(pageView);
                browse(user, random.nextInt(PageViewRandomSimulator.this.delay));
            }
        }, delay, TimeUnit.MILLISECONDS);
    }

}
