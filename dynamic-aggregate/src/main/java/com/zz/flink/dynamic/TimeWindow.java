package com.zz.flink.dynamic;

import java.util.List;

public interface TimeWindow {

    List<Long> assignWindows(long eventTime);

    long getSizeInMillis();

}
