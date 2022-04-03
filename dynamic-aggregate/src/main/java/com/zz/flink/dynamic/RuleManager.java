package com.zz.flink.dynamic;

import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public class RuleManager {

    private volatile Map<Integer, Rule> ruleMap = new HashMap<>();

    private volatile Map<String, List<Rule>> ruleMapByKey = new HashMap<>();

    private static RuleManager instance = new RuleManager();

    public static RuleManager getInstance() {
        return instance;
    }

    public RuleManager() {
        final List<Rule> allRules = new ArrayList<>();
//        Rule rule = new Rule();
//        rule.setId(1);
//        rule.setGroupKey("pageId");
//        rule.setWindow(new TumbleWindow(10, TimeUnit.SECONDS));
//        rule.addMetric(MetricInfo.count("count_by_page_10s"));
//        rule.addMetric(MetricInfo.sum("duration_sum_by_page_10s", "duration"));
//        rule.addMetric(MetricInfo.expr("duration_avg_by_page_10s", "duration_sum_by_page_10s/count_by_page_10s"));
//        allRules.add(rule);
//        rule = new Rule();
//        rule.setId(3);
//        rule.setGroupKey("userId");
//        rule.setWindow(new TumbleWindow(10, TimeUnit.SECONDS));
//        allRules.add(rule);
//        rule.addMetric(MetricInfo.count("count_by_user_10s"));
//        rule = new Rule();
//        rule.setId(4);
//        rule.setGroupKey("userId");
//        rule.setWindow(new TumbleWindow(30, TimeUnit.SECONDS));
//        rule.addMetric(MetricInfo.count("count_by_user_30s"));
//        rule.addMetric(MetricInfo.sum("duration_sum_by_user_30s", "duration"));
//        rule.addMetric(MetricInfo.expr("duration_avg_by_user_30s","duration_sum_by_user_30s/count_by_user_30s"));
//        allRules.add(rule);
        Rule rule = new Rule();
        rule.setId(5);
        rule.setGroupKey("userId");
        rule.setWindow(new SlideWindow(30, 10, TimeUnit.SECONDS));
        rule.addMetric(MetricInfo.count("count_by_user_30s_10s"));
        rule.addMetric(MetricInfo.sum("duration_sum_by_user_30s_10s", "duration"));
        rule.addMetric(MetricInfo.expr("duration_avg_by_user_30s_10s", "duration_sum_by_user_30s_10s/count_by_user_30s_10s"));
        allRules.add(rule);
        for (Rule r : allRules) {
            ruleMap.put(r.getId(), r);
            ruleMapByKey.computeIfAbsent(r.getGroupKey(), s -> new ArrayList<>()).add(r);
        }
//        Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(new Runnable() {
//            @Override
//            public void run() {
//                int r = new Random().nextInt(allRules.size());
//                Map<Integer, Rule> ruleMap = new HashMap<>();
//                Map<String, List<Rule>> ruleMapByKey = new HashMap<>();
//                for (int i = 0; i < r; i++) {
//                    Rule rule = allRules.get(i);
//                    ruleMap.put(rule.getId(), rule);
//                    ruleMapByKey.computeIfAbsent(rule.getGroupKey(), s -> new ArrayList<>()).add(rule);
//                }
//                RuleManager.this.ruleMap = ruleMap;
//                RuleManager.this.ruleMapByKey = ruleMapByKey;
//            }
//        }, 0, 60, TimeUnit.SECONDS);
    }

    public Map<String, List<Rule>> getRuleMapByKey() {
        return ruleMapByKey;
    }

    public Rule getRule(int ruleId) {
        return ruleMap.get(ruleId);
    }
}
