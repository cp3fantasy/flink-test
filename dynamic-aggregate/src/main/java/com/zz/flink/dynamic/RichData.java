package com.zz.flink.dynamic;

import java.util.List;
import java.util.Map;

public class RichData {

    private Map<String,Object> data;

    private String key;

    private List<Integer> ruleIds;

    public Map<String, Object> getData() {
        return data;
    }

    public void setData(Map<String, Object> data) {
        this.data = data;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public List<Integer> getRuleIds() {
        return ruleIds;
    }

    public void setRuleIds(List<Integer> ruleIds) {
        this.ruleIds = ruleIds;
    }
}
