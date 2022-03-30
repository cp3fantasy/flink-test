package com.zz.flink.dynamic;

public class Rule {

    private int id;

    private String groupKey;

    private String filterExpr;

    private String aggregatorType;

    private String aggregatorField;

    private TimeWindow window;

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

    public String getFilterExpr() {
        return filterExpr;
    }

    public void setFilterExpr(String filterExpr) {
        this.filterExpr = filterExpr;
    }

    public String getAggregatorType() {
        return aggregatorType;
    }

    public void setAggregatorType(String aggregatorType) {
        this.aggregatorType = aggregatorType;
    }

    public String getAggregatorField() {
        return aggregatorField;
    }

    public void setAggregatorField(String aggregatorField) {
        this.aggregatorField = aggregatorField;
    }

    public TimeWindow getWindow() {
        return window;
    }

    public void setWindow(TimeWindow window) {
        this.window = window;
    }
}
