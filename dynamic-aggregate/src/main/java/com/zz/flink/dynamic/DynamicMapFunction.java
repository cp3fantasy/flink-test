package com.zz.flink.dynamic;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DynamicMapFunction extends RichFlatMapFunction<Map<String, Object>, RichData> {


    @Override
    public void open(Configuration parameters) throws Exception {
    }

    @Override
    public void flatMap(Map<String, Object> data, Collector<RichData> out) throws Exception {
        Map<String, List<Rule>> ruleMap = RuleManager.getInstance().getRuleMapByKey();
        for (Map.Entry<String, List<Rule>> entry : ruleMap.entrySet()) {
            String groupKey = entry.getKey();
            String groupValue = data.get(groupKey).toString();
            RichData richData = new RichData();
            richData.setData(data);
            richData.setKey(groupValue);
            richData.setRuleIds(getRuleIds(entry.getValue()));
            out.collect(richData);
        }
    }

    private List<Integer> getRuleIds(List<Rule> rules) {
        List<Integer> ruleIds = new ArrayList<>(rules.size());
        for (Rule rule : rules) {
            ruleIds.add(rule.getId());
        }
        return ruleIds;
    }
}
