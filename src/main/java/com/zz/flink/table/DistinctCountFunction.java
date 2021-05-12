package com.zz.flink.table;

import org.apache.flink.table.functions.TableAggregateFunction;

import java.util.HashSet;

public class DistinctCountFunction extends TableAggregateFunction<Integer,DistinctCountAccumulator> {

    @Override
    public DistinctCountAccumulator createAccumulator() {
        DistinctCountAccumulator accumulator = new DistinctCountAccumulator();
        accumulator.set = new HashSet<>();
        return accumulator;
    }

    public void accumulate(DistinctCountAccumulator accumulator, String value){
        accumulator.set.add(value);
    }



}

