package com.zz.flink.common.simulator;

import com.zz.flink.common.model.PageView;

public interface PageViewHandler {

    void handle(PageView pageView);
}
