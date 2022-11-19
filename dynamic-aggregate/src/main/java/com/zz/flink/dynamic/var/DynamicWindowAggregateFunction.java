package com.zz.flink.dynamic.var;

import com.zz.flink.dynamic.CumulateWindow;
import com.zz.flink.dynamic.TimeWindow;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Map;

public class DynamicWindowAggregateFunction extends KeyedProcessFunction<String, RichData, AggregateResult> {

    private transient MapState<String, Aggregator> state;

    private MapStateDescriptor<String, Aggregator> stateDescriptor =
            new MapStateDescriptor<>(
                    "windowState",
                    BasicTypeInfo.STRING_TYPE_INFO,
                    TypeInformation.of(new TypeHint<Aggregator>() {
                    }));

    private transient DateFormat dateFormat;

    private VarManager varManager;

    @Override
    public void open(Configuration parameters) throws Exception {
        state = getRuntimeContext().getMapState(stateDescriptor);
        dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        varManager = new VarManager();
    }

    @Override
    public void processElement(RichData richData, Context ctx, Collector<AggregateResult> out) throws Exception {
        for (String varName : richData.getVarNames()) {
            StatVar var = varManager.getStatVar(varName);
            if (var != null) {
                Aggregator aggregator = state.get(var.getName());
                if (aggregator == null) {
                    aggregator = createAggregator(var);
                }
                if (aggregator != null) {
                    aggregator.aggregate(richData.getData(), var);
                    state.put(varName, aggregator);
                    out.collect(buildResult(varName, ctx.getCurrentKey(),aggregator.getResult()));
//            long emitTime = getExpireTime(ctx.timestamp(), var.getWindow());
//            ctx.timerService().registerEventTimeTimer(emitTime);
                }
            }
        }
    }


    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<AggregateResult> out) throws Exception {
//        List<AggregatorKey> keysToRemove = new ArrayList<>();
//        for (AggregatorKey aggregatorKey : state.keys()) {
//            String varName = aggregatorKey.getVarName();
//            StatVar var = varManager.getStatVar(varName);
//            if (var != null) {
//                if (shouldEmit(timestamp,aggregatorKey.getWindowStartTime(),var.getWindow())) {
//                    Aggregator aggregator = state.get(aggregatorKey);
//                    Object result = aggregator.getResult();
//                    out.collect(buildResult(varName, ctx.getCurrentKey(), aggregatorKey.getWindowStartTime(), result));
//                    keysToRemove.add(aggregatorKey);
//                }
//            } else {
//                keysToRemove.add(aggregatorKey);
//            }
//        }
//        for (AggregatorKey key : keysToRemove) {
//            state.remove(key);
//        }
    }

    private boolean shouldEmit(long timestamp, long windowStartTime, TimeWindow window) {
        if (window instanceof CumulateWindow) {
            CumulateWindow cumulateWindow = (CumulateWindow) window;
            return timestamp % cumulateWindow.getStepInMillis() == 0;
        }
        return timestamp == windowStartTime + window.getSizeInMillis();
    }

    private AggregateResult buildResult(String varName, String currentKey, Object result) {
        AggregateResult aggregateResult = new AggregateResult();
        aggregateResult.setVarName(varName);
        aggregateResult.setKey(currentKey);
        aggregateResult.setValue(result);
        return aggregateResult;
    }

    private long getExpireTime(long currentTime, TimeWindow window) {
        if (window instanceof CumulateWindow) {
            CumulateWindow cumulateWindow = (CumulateWindow) window;
            return (currentTime / cumulateWindow.getStepInMillis() + 1) * cumulateWindow.getStepInMillis();
        }
        return window.getSizeInMillis();
    }

    private Aggregator createAggregator(StatVar var) {
        if (var instanceof SumVar) {
            return new SumAggregator();
        } else if (var instanceof CountVar) {
            return new CountAggregator();
        } else if (var instanceof LastVar) {
            return new LastAggregator();
        }
        return null;
    }
}
