package com.zz.flink.dynamic.var2;

import java.util.Map;

public class SumAggregator implements Aggregator {

    private double sum;

    public double getSum() {
        return sum;
    }

    public void setSum(double sum) {
        this.sum = sum;
    }

    @Override
    public void aggregate(Map<String, Object> data, StatVar var) {
        SumVar sumVar = (SumVar) var;
        Object value = data.get(sumVar.getTargetVar());
        if (value != null) {
            sum += Double.parseDouble(value.toString());
        }
    }

    @Override
    public Object getResult() {
        return sum;
    }


}
