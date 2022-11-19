package com.zz.flink.dynamic.var2;

import com.zz.flink.dynamic.CumulateWindow;
import com.zz.flink.dynamic.TimeWindow;
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
import java.util.List;

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

    private VarManager varManager;

    @Override
    public void open(Configuration parameters) throws Exception {
        state = getRuntimeContext().getMapState(stateDescriptor);
        dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        varManager = new VarManager();
    }

    @Override
    public void processElement(RichData data, Context ctx, Collector<AggregateResult> out) throws Exception {
        for (String varName : data.getVarNames()) {
            StatVar var = varManager.getStatVar(varName);
            if (var != null) {
                processVar(data, var, ctx);
            }
        }
    }

    private void processVar(RichData richData, StatVar var, Context ctx) throws Exception {
        TimeWindow window = var.getWindow();
        List<Long> windowStartTimes = window.assignWindows(richData.getEventTime());
        for (long windowStartTime : windowStartTimes) {
            AggregatorKey aggregatorKey = new AggregatorKey();
            aggregatorKey.setVarName(var.getName());
            aggregatorKey.setWindowStartTime(windowStartTime);
            Aggregator aggregator = state.get(aggregatorKey);
            if (aggregator == null) {
                aggregator = createAggregator(var);
                if (aggregator != null) {
                    state.put(aggregatorKey, aggregator);
                    long emitTime = getEmitTime(ctx.timestamp(), windowStartTime, window);
                    ctx.timerService().registerEventTimeTimer(emitTime);
                }
            }
            if (aggregator != null) {
                aggregator.aggregate(richData.getData(), var);
            }
        }
    }


    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<AggregateResult> out) throws Exception {
        List<AggregatorKey> keysToRemove = new ArrayList<>();
        for (AggregatorKey aggregatorKey : state.keys()) {
            String varName = aggregatorKey.getVarName();
            StatVar var = varManager.getStatVar(varName);
            if (var != null) {
                if (shouldEmit(timestamp,aggregatorKey.getWindowStartTime(),var.getWindow())) {
                    Aggregator aggregator = state.get(aggregatorKey);
                    Object result = aggregator.getResult();
                    out.collect(buildResult(varName, ctx.getCurrentKey(), aggregatorKey.getWindowStartTime(), result));
                    keysToRemove.add(aggregatorKey);
                }
            } else {
                keysToRemove.add(aggregatorKey);
            }
        }
        for (AggregatorKey key : keysToRemove) {
            state.remove(key);
        }
    }

    private boolean shouldEmit(long timestamp, long windowStartTime, TimeWindow window) {
        if (window instanceof CumulateWindow) {
            CumulateWindow cumulateWindow = (CumulateWindow) window;
            return timestamp % cumulateWindow.getStepInMillis() == 0;
        }
        return timestamp == windowStartTime + window.getSizeInMillis();
    }

    private AggregateResult buildResult(String varName, String currentKey, long windowStartTime, Object result) {
        AggregateResult aggregateResult = new AggregateResult();
        aggregateResult.setVarName(varName);
        aggregateResult.setKey(currentKey);
        aggregateResult.setWindowTime(dateFormat.format(windowStartTime));
        aggregateResult.setValue(result);
        return aggregateResult;
    }

    private long getEmitTime(long currentTime, long windowStartTime, TimeWindow window) {
        if (window instanceof CumulateWindow) {
            CumulateWindow cumulateWindow = (CumulateWindow) window;
            return (currentTime / cumulateWindow.getStepInMillis() + 1) * cumulateWindow.getStepInMillis();
        }
        return windowStartTime + window.getSizeInMillis();
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
