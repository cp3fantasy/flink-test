package com.zz.flink.dynamic.var;

import java.util.LinkedList;
import java.util.Map;

public class LastAggregator implements Aggregator {

    private LinkedList<Object> dataList = new LinkedList<>();

    @Override
    public void aggregate(Map<String, Object> data, StatVar var) {
        LastVar lastVar = (LastVar) var;
        Object value = data.get(lastVar.getTargetVar());
        if (value != null) {
            dataList.addLast(value);
            if(dataList.size() > lastVar.getLastCount()){
                dataList.removeFirst();
            }
        }
    }

    @Override
    public Object getResult() {
        return dataList;
    }
}
