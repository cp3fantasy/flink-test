package com.zz.flink.dynamic.var2;

import java.util.Objects;

public class AggregatorKey {

    private String varName;

    private long windowStartTime;


    public String getVarName() {
        return varName;
    }

    public void setVarName(String varName) {
        this.varName = varName;
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
        return windowStartTime == that.windowStartTime && Objects.equals(varName, that.varName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(varName, windowStartTime);
    }
}
