package com.zz.flink.dynamic.var2;

import com.alibaba.fastjson.JSON;

public class AggregateResult {

    private String varName;

    private String windowTime;

    private String key;

    private Object value;

    public String getVarName() {
        return varName;
    }

    public void setVarName(String varName) {
        this.varName = varName;
    }

    public String getWindowTime() {
        return windowTime;
    }

    public void setWindowTime(String windowTime) {
        this.windowTime = windowTime;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return JSON.toJSONString(this);
    }
}
