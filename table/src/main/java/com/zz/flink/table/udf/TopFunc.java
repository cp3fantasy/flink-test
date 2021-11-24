package com.zz.flink.table.udf;

import org.apache.flink.table.functions.AggregateFunction;

public class TopFunc<T extends Comparable<T>> extends AggregateFunction<T, TopFunc.TopAcc<T>> {

    @Override
    public T getValue(TopAcc<T> accumulator) {
        return accumulator.max;
    }

    @Override
    public TopAcc<T> createAccumulator() {
        return new TopAcc<>();
    }

    public void accumulate(TopAcc<T> accumulator, T value) {
        if (accumulator.max == null || value.compareTo(accumulator.max) > 0) {
            accumulator.max = value;
        }
    }

    public static class TopAcc<T extends Comparable<T>> {
        T max;
    }
}
