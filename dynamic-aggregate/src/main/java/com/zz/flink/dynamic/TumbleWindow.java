package com.zz.flink.dynamic;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class TumbleWindow implements TimeWindow {

    private int size;

    private TimeUnit timeUnit;

    private long sizeInMillis;

    public TumbleWindow(int size, TimeUnit timeUnit) {
        this.size = size;
        this.timeUnit = timeUnit;
        this.sizeInMillis = timeUnit.toMillis(size);
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

    @Override
    public String toString() {
        return "TumbleWindow{" +
                "size=" + size +
                ", timeUnit=" + timeUnit +
                '}';
    }
}
