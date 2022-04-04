package com.zz.flink.dynamic;

import com.googlecode.aviator.AviatorEvaluator;
import com.googlecode.aviator.Expression;

import java.util.ArrayList;
import java.util.List;

public class Rule {

    private int id;

    private String groupKey;

    private String filter;

    private Expression filterExpression;

    private TimeWindow window;

    private List<MetricInfo> basicMetrics = new ArrayList<>();

    private List<MetricInfo> exprMetrics = new ArrayList<>();

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getGroupKey() {
        return groupKey;
    }

    public void setGroupKey(String groupKey) {
        this.groupKey = groupKey;
    }

    public String getFilter() {
        return filter;
    }

    public void setFilter(String filter) {
        this.filter = filter;
        if (filter != null) {
            this.filterExpression = AviatorEvaluator.compile(filter, true);
        }
    }

    public List<MetricInfo> getBasicMetrics() {
        return basicMetrics;
    }

    public List<MetricInfo> getExprMetrics() {
        return exprMetrics;
    }

    public void addMetric(MetricInfo metricInfo) {
        if (metricInfo.getType().equals("expr")) {
            exprMetrics.add(metricInfo);
        } else {
            basicMetrics.add(metricInfo);
        }
    }


    public TimeWindow getWindow() {
        return window;
    }

    public void setWindow(TimeWindow window) {
        this.window = window;
    }

    public Expression getFilterExpression() {
        return filterExpression;
    }

}
