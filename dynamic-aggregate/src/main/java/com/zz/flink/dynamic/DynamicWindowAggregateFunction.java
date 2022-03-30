package com.zz.flink.dynamic;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class DynamicWindowAggregateFunction extends KeyedProcessFunction<String, RichData, AggregateResult> {

    private transient MapState<AggregatorKey, Aggregator> state;

    private MapStateDescriptor<AggregatorKey, Aggregator> stateDescriptor =
            new MapStateDescriptor<>(
                    "windowState",
                    TypeInformation.of(new TypeHint<AggregatorKey>() {
                    }),
                    TypeInformation.of(new TypeHint<Aggregator>() {
                    }));

    private transient DateFormat dateFormat;

    @Override
    public void open(Configuration parameters) throws Exception {
        state = getRuntimeContext().getMapState(stateDescriptor);
        dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    }

    @Override
    public void processElement(RichData data, Context ctx, Collector<AggregateResult> out) throws Exception {
        for (int ruleId : data.getRuleIds()) {
            Rule rule = RuleManager.getInstance().getRule(ruleId);
            if (rule != null) {
                processRule(data, rule, ctx);
            }
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<AggregateResult> out) throws Exception {
        List<AggregatorKey> keysToRemove = new ArrayList<>();
        for (AggregatorKey aggregatorKey : state.keys()) {
            int ruleId = aggregatorKey.getRuleId();
            Rule rule = RuleManager.getInstance().getRule(ruleId);
            if (rule != null) {
                long emitTime = getEmitTime(aggregatorKey.getWindowStartTime(), rule.getWindow());
                if (emitTime == timestamp) {
                    AggregateResult aggregateResult = new AggregateResult();
                    aggregateResult.setRuleId(ruleId);
                    aggregateResult.setKey(ctx.getCurrentKey());
                    aggregateResult.setValue(state.get(aggregatorKey).getResult());
                    aggregateResult.setWindowTime(dateFormat.format(aggregatorKey.getWindowStartTime()));
                    out.collect(aggregateResult);
                    keysToRemove.add(aggregatorKey);
                }
            }
        }
        for (AggregatorKey key : keysToRemove) {
            state.remove(key);
        }
    }

    private void processRule(RichData richData, Rule rule, Context ctx) throws Exception {
        TimeWindow window = rule.getWindow();
        List<Long> windowStartTimes = window.assignWindows(richData.getEventTime());
        for (long windowStartTime : windowStartTimes) {
            AggregatorKey aggregatorKey = new AggregatorKey();
            aggregatorKey.setRuleId(rule.getId());
            aggregatorKey.setWindowStartTime(windowStartTime);
            Aggregator aggregator = state.get(aggregatorKey);
            if (aggregator == null) {
                aggregator = createAggregator(rule);
                state.put(aggregatorKey, aggregator);
                long emitTime = getEmitTime(windowStartTime, window);
                ctx.timerService().registerEventTimeTimer(emitTime);
            }
            aggregator.aggregate(richData.getData());
        }
    }

    private long getEmitTime(long windowStartTime, TimeWindow window) {
        return windowStartTime + window.getSizeInMillis();
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
