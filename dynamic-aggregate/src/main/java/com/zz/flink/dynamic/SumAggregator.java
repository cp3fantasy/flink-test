package com.zz.flink.dynamic;

import com.googlecode.aviator.AviatorEvaluator;

import java.util.Map;

public class SumAggregator implements Aggregator {

    private double sum;

    private String expr;

    public SumAggregator(String expr) {
        this.expr = expr;
    }

    public SumAggregator() {
    }

    public double getSum() {
        return sum;
    }

    public void setSum(double sum) {
        this.sum = sum;
    }

    public String getExpr() {
        return expr;
    }

    public void setExpr(String expr) {
        this.expr = expr;
    }

    @Override
    public void aggregate(Map<String, Object> data) {
        Object value = AviatorEvaluator.execute(expr, data, true);
        if (value != null) {
            sum += Double.parseDouble(value.toString());
        }
    }

    @Override
    public Object getResult() {
        return sum;
    }


}
