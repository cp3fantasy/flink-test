package com.zz.flink.common.model;

import com.zz.flink.common.util.TimeUtil;

public class PageView {

    private String pageId;

    private String userId;

    private long startTime;

    public String getPageId() {
        return pageId;
    }

    public void setPageId(String pageId) {
        this.pageId = pageId;
    }

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

    @Override
    public String toString() {
        return "PageView{" +
                "pageId='" + pageId + '\'' +
                ", userId='" + userId + '\'' +
                ", startTime=" + TimeUtil.format(startTime) +
                '}';
    }
}
