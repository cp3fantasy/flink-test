package com.zz.flink.common.kafka;

import com.zz.flink.common.simulator.PageViewFixedSpeedSimulator;

public class PageViewFixedSpeedKafkaProducer {

    public static void main(String[] args) {
        PageViewKafkaProducer producer = new PageViewKafkaProducer();
        PageViewFixedSpeedSimulator simulator = new PageViewFixedSpeedSimulator(3, 5);
        simulator.setHandler(producer);
        simulator.start();
    }
}
