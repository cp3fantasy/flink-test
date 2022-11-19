package com.zz.flink.dynamic.rule;

import com.googlecode.aviator.Expression;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DynamicMapFunction extends RichFlatMapFunction<Map<String, Object>, RichData> {

    private transient String timeField;


    @Override
    public void open(Configuration config) throws Exception {
        ParameterTool parameters = (ParameterTool)
                getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        this.timeField = parameters.get("timeField","startTime");
        System.out.println("timeField:"+timeField);
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
            richData.setRuleIds(getRuleIds(data, entry.getValue()));
            richData.setEventTime((Long) data.get(timeField));
            out.collect(richData);
        }
    }

    private List<Integer> getRuleIds(Map<String, Object> data, List<Rule> rules) {
        List<Integer> ruleIds = new ArrayList<>(rules.size());
        for (Rule rule : rules) {
            Expression filterExpression = rule.getFilterExpression();
            if (filterExpression != null) {
                if ((Boolean) filterExpression.execute(data)) {
                    ruleIds.add(rule.getId());
                }
            } else {
                ruleIds.add(rule.getId());
            }
        }
        return ruleIds;
    }
}
