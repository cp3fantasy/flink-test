package com.zz.flink.dynamic;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class CumulateWindow implements TimeWindow{
    private int size;

    private int step;

    private TimeUnit timeUnit;

    private long sizeInMillis;

    private long stepInMillis;

    private int stepCount;

    public CumulateWindow(int size, int step, TimeUnit timeUnit) {
        this.size = size;
        this.step = step;
        this.timeUnit = timeUnit;
        this.sizeInMillis = timeUnit.toMillis(size);
        this.stepInMillis = timeUnit.toMillis(step);
        this.stepCount = size / step;
    }

    @Override
    public List<Long> assignWindows(long eventTime) {
        long startTime = eventTime / sizeInMillis * sizeInMillis;
        return Collections.singletonList(startTime);
    }

    @Override
    public long getSizeInMillis() {
        return sizeInMillis;
    }

    public long getStepInMillis() {
        return stepInMillis;
    }

    @Override
    public String toString() {
        return "CumulateWindow{" +
                "size=" + size +
                ", step=" + step +
                ", timeUnit=" + timeUnit +
                '}';
    }
}
