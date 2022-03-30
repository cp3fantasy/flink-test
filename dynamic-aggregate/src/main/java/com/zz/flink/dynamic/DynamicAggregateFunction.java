package com.zz.flink.dynamic;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

public class DynamicAggregateFunction extends KeyedProcessFunction<String, RichData, AggregateResult> {

    private transient MapState<Integer, Aggregator> state;

    private MapStateDescriptor<Integer, Aggregator> stateDescriptor =
            new MapStateDescriptor<>(
                    "windowState",
                    BasicTypeInfo.INT_TYPE_INFO,
                    TypeInformation.of(new TypeHint<Aggregator>() {
                    }));

    @Override
    public void open(Configuration parameters) throws Exception {
        state = getRuntimeContext().getMapState(stateDescriptor);
    }

    @Override
    public void processElement(RichData data, Context ctx, Collector<AggregateResult> out) throws Exception {
        for (int ruleId : data.getRuleIds()) {
            Rule rule = RuleManager.getInstance().getRule(ruleId);
            if (rule != null) {
                Object result = processRule(data.getData(), rule);
                AggregateResult aggregateResult = new AggregateResult();
                aggregateResult.setKey(ctx.getCurrentKey());
                aggregateResult.setValue(result);
                aggregateResult.setWindowTime(new SimpleDateFormat("yyyyMMdd HH:mm:ss").format(new Date()));
                out.collect(aggregateResult);
            }
        }
    }

    private Object processRule(Map<String, Object> data, Rule rule) throws Exception {
        Aggregator aggregator = state.get(rule.getId());
        if (aggregator == null) {
            aggregator = createAggregator(rule);
            state.put(rule.getId(), aggregator);
        }
        aggregator.aggregate(data);
//        System.out.println(aggregator.getResult());
        return aggregator.getResult();
    }

    private Aggregator createAggregator(Rule rule) {
        String type = rule.getAggregatorType();
        if (type.equals("sum")) {
            return new SumAggregator(rule.getAggregatorField());
        } else {
            return new CountAggregator();
        }
    }
}
