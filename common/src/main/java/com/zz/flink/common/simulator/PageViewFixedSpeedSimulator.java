package com.zz.flink.common.simulator;

import com.zz.flink.common.model.PageView;
import com.zz.flink.common.util.SimulateUtil;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import io.netty.util.Timer;
import io.netty.util.TimerTask;

import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class PageViewFixedSpeedSimulator {

    private AtomicInteger seqNo = new AtomicInteger();

    private int limit;

    private String[] users;

    private String[] pageIds;

    private Timer timer;

    private PageViewHandler handler;

    public PageViewFixedSpeedSimulator(int userCount, int pageCount, int limit) {
        users = SimulateUtil.createStringArray("user", userCount);
        pageIds = SimulateUtil.createStringArray("page", pageCount);
        timer = new HashedWheelTimer();
        this.limit = limit;
    }

    public PageViewFixedSpeedSimulator(int userCount, int pageCount) {
        this(userCount, pageCount, -1);
    }

    public void setHandler(PageViewHandler handler) {
        this.handler = handler;
    }

    public void start() {
        for (String user : users) {
            visitPage(user, 1);
        }
    }

    private void visitPage(String user, int count) {
        TimerTask task = new TimerTask() {

            @Override
            public void run(Timeout timeout) throws Exception {
                PageView pageView = new PageView();
                pageView.setSeqNo(String.valueOf(seqNo.incrementAndGet()));
                pageView.setUserId(user);
                pageView.setPageId(pageIds[count % pageIds.length]);
                pageView.setStartTime(System.currentTimeMillis());
                int duration = new Random().nextInt(1000);
                pageView.setDuration(duration);
                pageView.setEndTime(pageView.getStartTime() + duration);
                System.out.println(pageView);
                if (handler != null) {
                    handler.handle(pageView);
                }
                if (limit <= 0 || count < limit) {
                    visitPage(user, count + 1);
                }
            }
        };
        timer.newTimeout(task, 1000, TimeUnit.MILLISECONDS);
    }

    public static void main(String[] args) {
        new PageViewFixedSpeedSimulator(2, 5, 10).start();
    }

}
