package com.zz.flink.dynamic;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class SlideWindow implements TimeWindow {

    private int size;

    private int step;

    private TimeUnit timeUnit;

    private long sizeInMillis;

    private long stepInMillis;

    private int stepCount;

    public SlideWindow(int size, int step, TimeUnit timeUnit) {
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
        List<Long> windowStartTimes = new ArrayList<>(stepCount);
        for (int i = 0; i < stepCount; i++) {
            windowStartTimes.add(startTime);
            startTime += stepInMillis;
        }
        return windowStartTimes;
    }

    @Override
    public long getSizeInMillis() {
        return sizeInMillis;
    }


}
