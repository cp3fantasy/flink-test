package com.zz.flink.streaming;

import com.zz.flink.common.model.PageView;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

public class LocalAggFunction extends ProcessFunction<PageView, Tuple2<String, Integer>> implements CheckpointedFunction {

    private Map<String, Integer> localMap;

    @Override
    public void open(Configuration parameters) throws Exception {
        localMap = new HashMap<>();
    }

    @Override
    public void processElement(PageView pageView, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
        Integer count = localMap.get(pageView.getUserId());
        count = count == null ? 1 : count + 1;
        if (count == 3) {
            out.collect(new Tuple2<>(pageView.getUserId(), count));
            localMap.remove(pageView.getUserId());
        } else {
            localMap.put(pageView.getUserId(), count);
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {

    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {

    }

}
