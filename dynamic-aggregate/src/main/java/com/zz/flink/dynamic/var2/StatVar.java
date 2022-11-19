package com.zz.flink.dynamic.var2;

import com.googlecode.aviator.AviatorEvaluator;
import com.googlecode.aviator.Expression;
import com.zz.flink.dynamic.TimeWindow;

public class StatVar extends Var {

    private String groupKey;

    private String filter;

    private Expression filterExpression;

    private TimeWindow window;

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
            filterExpression = AviatorEvaluator.compile(filter, true);
        }
    }

    public Expression getFilterExpression() {
        return filterExpression;
    }

    public void setFilterExpression(Expression filterExpression) {
        this.filterExpression = filterExpression;
    }

    public TimeWindow getWindow() {
        return window;
    }

    public void setWindow(TimeWindow window) {
        this.window = window;
    }

}
