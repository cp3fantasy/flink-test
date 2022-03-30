package com.zz.flink.dynamic;

import com.alibaba.fastjson.JSON;

public class AggregateResult {

    private int ruleId;

    private String windowTime;

    private String key;

    private Object value;

    public int getRuleId() {
        return ruleId;
    }

    public void setRuleId(int ruleId) {
        this.ruleId = ruleId;
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
