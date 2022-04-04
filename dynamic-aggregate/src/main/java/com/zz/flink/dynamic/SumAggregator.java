package com.zz.flink.dynamic;

import com.googlecode.aviator.AviatorEvaluator;
import com.googlecode.aviator.Expression;

import java.util.Map;

public class SumAggregator implements Aggregator {

    private double sum;

    private Expression expression;

    public SumAggregator(Expression expression) {
        this.expression = expression;
    }

    public double getSum() {
        return sum;
    }

    public void setSum(double sum) {
        this.sum = sum;
    }

    @Override
    public void aggregate(Map<String, Object> data) {
        Object value = expression.execute(data);
        if (value != null) {
            sum += Double.parseDouble(value.toString());
        }
    }

    @Override
    public Object getResult() {
        return sum;
    }


}
