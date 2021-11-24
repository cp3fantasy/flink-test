package com.zz.flink.common.model;

import java.util.ArrayList;
import java.util.List;

public class PvBatch {

    private String userId;

    private long startTime;

    private List<PageView> pageViews = new ArrayList<>();

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public List<PageView> getPageViews() {
        return pageViews;
    }

    public void setPageViews(List<PageView> pageViews) {
        this.pageViews = pageViews;
    }

    public void addPageView(PageView pageView){
        pageViews.add(pageView);
    }

}
