package com.zz.flink.common.kafka;

import com.zz.flink.common.simulator.PageViewRandomSimulator;

public class PageViewRandomKafkaProducer {

    public static void main(String[] args) {
        PageViewKafkaProducer producer = new PageViewKafkaProducer();
        PageViewRandomSimulator simulator = new PageViewRandomSimulator(3,10,5000);
        simulator.setHandler(producer);
        simulator.start();
    }
}
