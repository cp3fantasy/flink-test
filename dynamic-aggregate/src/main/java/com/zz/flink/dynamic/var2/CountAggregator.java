package com.zz.flink.dynamic.var2;

import java.util.Map;

public class CountAggregator implements Aggregator {

    private long count;

    @Override
    public void aggregate(Map<String, Object> data, StatVar var) {
        count++;
    }

    @Override
    public Object getResult() {
        return count;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }
}
