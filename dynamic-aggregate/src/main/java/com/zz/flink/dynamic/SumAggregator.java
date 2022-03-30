package com.zz.flink.dynamic;

import java.util.Map;

public class SumAggregator implements Aggregator {

    private double sum;

    private String field;

    public SumAggregator(String field) {
        this.field = field;
    }

    public SumAggregator() {
    }

    public double getSum() {
        return sum;
    }

    public void setSum(double sum) {
        this.sum = sum;
    }

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }

    @Override
    public void aggregate(Map<String, Object> data) {
        Double value = Double.parseDouble(data.get(field).toString());
        sum += value;
    }

    @Override
    public Object getResult() {
        return sum;
    }


}
