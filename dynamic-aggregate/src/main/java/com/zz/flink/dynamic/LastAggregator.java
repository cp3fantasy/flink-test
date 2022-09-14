package com.zz.flink.dynamic;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class LastAggregator implements Aggregator {

    private int lastCount;

    private String field;

    private LinkedList<Object> dataList;

    public LastAggregator(int lastCount,String field) {
        this.lastCount = lastCount;
        this.field = field;
        dataList = new LinkedList<>();
    }

    @Override
    public void aggregate(Map<String, Object> data) {
        Object value = data.get(field);
        if (value != null) {
            dataList.addLast(value);
            if(dataList.size()>lastCount){
                dataList.removeFirst();
            }
        }
    }

    @Override
    public Object getResult() {
        return dataList;
    }
}
