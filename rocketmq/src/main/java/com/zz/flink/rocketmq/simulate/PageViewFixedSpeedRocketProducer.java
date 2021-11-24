package com.zz.flink.rocketmq.simulate;

import com.zz.flink.common.simulator.PageViewFixedSpeedSimulator;

public class PageViewFixedSpeedRocketProducer {

    public static void main(String[] args) {
        PageViewRocketProducer producer = new PageViewRocketProducer();
        PageViewFixedSpeedSimulator simulator = new PageViewFixedSpeedSimulator(3, 5, 100);
        simulator.setHandler(producer);
        simulator.start();
    }
}
