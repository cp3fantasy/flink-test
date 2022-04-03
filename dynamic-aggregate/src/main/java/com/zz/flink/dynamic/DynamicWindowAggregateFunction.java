package com.zz.flink.dynamic;

import com.googlecode.aviator.AviatorEvaluator;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

public class DynamicWindowAggregateFunction extends KeyedProcessFunction<String, RichData, AggregateResult> {

    private transient MapState<AggregatorKey, List<Aggregator>> state;

    private MapStateDescriptor<AggregatorKey, List<Aggregator>> stateDescriptor =
            new MapStateDescriptor<>(
                    "windowState",
                    TypeInformation.of(new TypeHint<AggregatorKey>() {
                    }),
                    TypeInformation.of(new TypeHint<List<Aggregator>>() {
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

    private void processRule(RichData richData, Rule rule, Context ctx) throws Exception {
        TimeWindow window = rule.getWindow();
        List<Long> windowStartTimes = window.assignWindows(richData.getEventTime());
        for (long windowStartTime : windowStartTimes) {
            AggregatorKey aggregatorKey = new AggregatorKey();
            aggregatorKey.setRuleId(rule.getId());
            aggregatorKey.setWindowStartTime(windowStartTime);
            List<Aggregator> aggregators = state.get(aggregatorKey);
            if (aggregators == null) {
                aggregators = createAggregators(rule);
                state.put(aggregatorKey, aggregators);
                long emitTime = getEmitTime(windowStartTime, window);
                ctx.timerService().registerEventTimeTimer(emitTime);
            }
            for (Aggregator aggregator : aggregators) {
                aggregator.aggregate(richData.getData());
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
                    keysToRemove.add(aggregatorKey);
                    List<Aggregator> aggregators = state.get(aggregatorKey);
                    List<MetricInfo> basicMetrics = rule.getBasicMetrics();
                    List<MetricInfo> exprMetrics = rule.getExprMetrics();
                    Map<String, Object> env = null;
                    if (exprMetrics.size() > 0) {
                        env = new HashMap<>();
                    }
                    for (int i = 0; i < basicMetrics.size(); i++) {
                        Aggregator aggregator = aggregators.get(i);
                        Object result = aggregator.getResult();
                        MetricInfo metricInfo = basicMetrics.get(i);
                        if (metricInfo.isOutput()) {
                            out.collect(buildResult(ruleId, metricInfo.getName(), ctx.getCurrentKey(),
                                    aggregatorKey.getWindowStartTime(), result));
                        }
                        if (env != null) {
                            env.put(metricInfo.getName(), result);
                        }
                    }
                    for (MetricInfo metricInfo : rule.getExprMetrics()) {
                        Object result = AviatorEvaluator.execute(metricInfo.getExpr(), env, true);
                        env.put(metricInfo.getName(), result);
                        if (metricInfo.isOutput()) {
                            out.collect(buildResult(ruleId, metricInfo.getName(), ctx.getCurrentKey(),
                                    aggregatorKey.getWindowStartTime(), result));
                        }
                    }
                }
            } else {
                keysToRemove.add(aggregatorKey);
            }
        }
        for (AggregatorKey key : keysToRemove) {
            state.remove(key);
        }
    }

    private AggregateResult buildResult(int ruleId, String metricName, String currentKey, long windowStartTime, Object result) {
        AggregateResult aggregateResult = new AggregateResult();
        aggregateResult.setRuleId(ruleId);
        aggregateResult.setMetric(metricName);
        aggregateResult.setKey(currentKey);
        aggregateResult.setWindowTime(dateFormat.format(windowStartTime));
        aggregateResult.setValue(result);
        return aggregateResult;
    }

    private long getEmitTime(long windowStartTime, TimeWindow window) {
        return windowStartTime + window.getSizeInMillis();
    }

    private List<Aggregator> createAggregators(Rule rule) {
        List<MetricInfo> metricInfos = rule.getBasicMetrics();
        if (metricInfos.size() == 1) {
            return Collections.singletonList(createAggregator(metricInfos.get(0)));
        }
        List<Aggregator> aggregators = new ArrayList<>(metricInfos.size());
        for (MetricInfo metricInfo : metricInfos) {
            aggregators.add(createAggregator(metricInfo));
        }
        return aggregators;
    }

    private Aggregator createAggregator(MetricInfo metricInfo) {
        String type = metricInfo.getType();
        if (type.equals("sum")) {
            return new SumAggregator(metricInfo.getExpr());
        } else {
            return new CountAggregator();
        }
    }
}
