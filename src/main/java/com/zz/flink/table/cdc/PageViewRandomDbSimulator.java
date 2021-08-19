package com.zz.flink.table.cdc;

import com.zz.flink.common.simulator.PageViewRandomSimulator;

public class PageViewRandomDbSimulator {

    public static void main(String[] args) {
        PageViewDbWriter writer = new PageViewDbWriter();
        PageViewRandomSimulator simulator = new PageViewRandomSimulator(3, 5,10000);
        simulator.setHandler(writer);
        simulator.start();

    }
}
