package com.zz.flink.dynamic;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public class RuleManager {

    private volatile Map<Integer,Rule> ruleMap = new HashMap<>();

    private volatile Map<String, List<Rule>> ruleMapByKey = new HashMap<>();

    private static RuleManager instance = new RuleManager();

    public static RuleManager getInstance() {
        return instance;
    }

    public RuleManager() {
        ruleMapByKey = new HashMap<>();
        Rule rule = new Rule();
        rule.setId(1);
        rule.setGroupKey("pageId");
        rule.setAggregatorType("count");
        rule.setWindow(new TumbleWindow(10, TimeUnit.SECONDS));
        addRule(rule);
        rule = new Rule();
        rule.setId(2);
        rule.setGroupKey("pageId");
        rule.setAggregatorType("sum");
        rule.setAggregatorField("duration");
        rule.setWindow(new TumbleWindow(10, TimeUnit.SECONDS));
        addRule(rule);
        rule = new Rule();
        rule.setId(3);
        rule.setGroupKey("userId");
        rule.setAggregatorType("count");
        rule.setWindow(new TumbleWindow(10, TimeUnit.SECONDS));
        addRule(rule);
        rule = new Rule();
        rule.setId(4);
        rule.setGroupKey("userId");
        rule.setAggregatorType("count");
        rule.setWindow(new TumbleWindow(30, TimeUnit.SECONDS));
        addRule(rule);
    }

    private void addRule(Rule rule) {
        ruleMap.put(rule.getId(),rule);
        ruleMapByKey.computeIfAbsent(rule.getGroupKey(), s -> new ArrayList<>()).add(rule);
    }

    public Map<String, List<Rule>> getRuleMapByKey() {
        return ruleMapByKey;
    }

    public Rule getRule(int ruleId) {
        return ruleMap.get(ruleId);
    }
}
