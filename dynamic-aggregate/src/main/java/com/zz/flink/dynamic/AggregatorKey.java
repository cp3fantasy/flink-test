package com.zz.flink.dynamic;

import java.util.Objects;

public class AggregatorKey {

    private int ruleId;

    private long windowStartTime;

    public int getRuleId() {
        return ruleId;
    }

    public void setRuleId(int ruleId) {
        this.ruleId = ruleId;
    }

    public long getWindowStartTime() {
        return windowStartTime;
    }

    public void setWindowStartTime(long windowStartTime) {
        this.windowStartTime = windowStartTime;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AggregatorKey that = (AggregatorKey) o;
        return ruleId == that.ruleId &&
                windowStartTime == that.windowStartTime;
    }

    @Override
    public int hashCode() {
        return Objects.hash(ruleId, windowStartTime);
    }
}
