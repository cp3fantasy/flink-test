package com.zz.flink.dynamic;

import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class RuleManager {

    private volatile Map<Integer, Rule> ruleMap = new HashMap<>();

    private volatile Map<String, List<Rule>> ruleMapByKey = new HashMap<>();

    private String configUrl;

    public static void init(String configUrl) {
        instance = new RuleManager(configUrl);
    }

    private static RuleManager instance;

    public static RuleManager getInstance() {
        return instance;
    }

    public RuleManager(String configUrl) {
        this.configUrl = configUrl;
        Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                updateRules();
            }
        }, 0, 60, TimeUnit.SECONDS);
    }

    private void updateRules() {
        Map<Integer, Rule> ruleMap = new HashMap<>();
        Map<String, List<Rule>> ruleMapByKey = new HashMap<>();
        System.out.println("update config:" + configUrl);
        Rule rule = new Rule();
        rule.setId(1);
        rule.setGroupKey("pageId");
        rule.setWindow(new TumbleWindow(10, TimeUnit.SECONDS));
        rule.addMetric(MetricInfo.count("count_by_page_10s"));
        rule.addMetric(MetricInfo.sum("duration_sum_by_page_10s", "duration"));
        rule.addMetric(MetricInfo.expr("duration_avg_by_page_10s", "duration_sum_by_page_10s/count_by_page_10s"));
        addRule(rule, ruleMap, ruleMapByKey);
        rule = new Rule();
        rule.setId(3);
        rule.setGroupKey("userId");
        rule.setWindow(new TumbleWindow(10, TimeUnit.SECONDS));
        addRule(rule, ruleMap, ruleMapByKey);
        rule.addMetric(MetricInfo.count("count_by_user_10s"));
        rule = new Rule();
        rule.setId(4);
        rule.setGroupKey("userId");
        rule.setWindow(new TumbleWindow(30, TimeUnit.SECONDS));
        rule.addMetric(MetricInfo.count("count_by_user_30s"));
        rule.addMetric(MetricInfo.sum("duration_sum_by_user_30s", "duration"));
        rule.addMetric(MetricInfo.expr("duration_avg_by_user_30s", "duration_sum_by_user_30s/count_by_user_30s"));
        rule.addMetric(MetricInfo.last("last3pageIds_by_user_30s","pageId",3));
        addRule(rule, ruleMap, ruleMapByKey);
        rule = new Rule();
        rule.setId(5);
        rule.setGroupKey("userId");
        rule.setFilter("duration>500");
        rule.setWindow(new SlideWindow(30, 10, TimeUnit.SECONDS));
        rule.addMetric(MetricInfo.count("count_by_user_30s_10s"));
        rule.addMetric(MetricInfo.sum("duration_sum_by_user_30s_10s", "duration"));
        rule.addMetric(MetricInfo.expr("duration_avg_by_user_30s_10s", "duration_sum_by_user_30s_10s/count_by_user_30s_10s"));
        addRule(rule, ruleMap, ruleMapByKey);
        System.out.println("before update:" + this.ruleMap);
        this.ruleMap = ruleMap;
        this.ruleMapByKey = ruleMapByKey;
        System.out.println("after update:" + this.ruleMap);
    }

    private void addRule(Rule rule, Map<Integer, Rule> ruleMap, Map<String, List<Rule>> ruleMapByKey) {
        if (new Random().nextInt(10) > 1) {
            ruleMap.put(rule.getId(), rule);
            ruleMapByKey.computeIfAbsent(rule.getGroupKey(), s -> new ArrayList<>()).add(rule);
        }
    }


    public Map<String, List<Rule>> getRuleMapByKey() {
        return ruleMapByKey;
    }

    public Rule getRule(int ruleId) {
        return ruleMap.get(ruleId);
    }
}
