package com.zz.flink.dynamic.var2;

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

    private VarManager varManager;

    @Override
    public void open(Configuration config) throws Exception {
        ParameterTool parameters = (ParameterTool)
                getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        this.timeField = parameters.get("timeField", "startTime");
        System.out.println("timeField:" + timeField);
        varManager = new VarManager();
    }

    @Override
    public void flatMap(Map<String, Object> data, Collector<RichData> out) throws Exception {
        Map<String, List<StatVar>> map = varManager.getStatVarMapByGroupKey();
        for (Map.Entry<String, List<StatVar>> entry : map.entrySet()) {
            List<String> varNames = getVarNames(data, entry.getValue());
            if (varNames.size() > 0) {
                String groupKey = entry.getKey();
                String groupValue = data.get(groupKey).toString();
                RichData richData = new RichData();
                richData.setData(data);
                richData.setKey(groupValue);
                richData.setVarNames(varNames);
                richData.setEventTime((Long) data.get(timeField));
                out.collect(richData);
            }
        }

    }

    private List<String> getVarNames(Map<String, Object> data, List<StatVar> vars) {
        List<String> varNames = new ArrayList<>();
        for (StatVar var : vars) {
            Expression filterExpression = var.getFilterExpression();
            if (filterExpression != null) {
                if ((Boolean) filterExpression.execute(data)) {
                    varNames.add(var.getName());
                }
            } else {
                varNames.add(var.getName());
            }
        }
        return varNames;
    }

}
