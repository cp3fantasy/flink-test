package com.zz.flink.dynamic;

import com.googlecode.aviator.AviatorEvaluator;
import com.googlecode.aviator.Expression;


public class MetricInfo {

    private String name;

    private String type;

    private String expr;

    private Expression expression;

    private boolean output = true;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getExpr() {
        return expr;
    }

    public void setExpr(String expr) {
        this.expr = expr;
        if (expr != null) {
            expression = AviatorEvaluator.compile(expr, true);
        }
    }

    public Expression getExpression() {
        return expression;
    }

    public boolean isOutput() {
        return output;
    }

    public void setOutput(boolean output) {
        this.output = output;
    }

    public static MetricInfo count(String name) {
        MetricInfo metricInfo = new MetricInfo();
        metricInfo.setName(name);
        metricInfo.setType("count");
        return metricInfo;
    }

    public static MetricInfo sum(String name, String expr) {
        MetricInfo metricInfo = new MetricInfo();
        metricInfo.setName(name);
        metricInfo.setType("sum");
        metricInfo.setExpr(expr);
        return metricInfo;
    }

    public static MetricInfo last(String name, String field, int n) {
        MetricInfo metricInfo = new MetricInfo();
        metricInfo.setName(name);
        metricInfo.setType("last" + n);
        metricInfo.setExpr(field);
        return metricInfo;
    }

    public static MetricInfo expr(String name, String expr) {
        MetricInfo metricInfo = new MetricInfo();
        metricInfo.setName(name);
        metricInfo.setType("expr");
        metricInfo.setExpr(expr);
        return metricInfo;
    }


}
