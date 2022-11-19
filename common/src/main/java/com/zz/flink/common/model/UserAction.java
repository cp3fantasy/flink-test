package com.zz.flink.common.model;

public class UserAction {

    private long id;

    private String user;

    private String item;

    private String itemType;

    private String action;

    private String traceId;

    private long time;

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getItem() {
        return item;
    }

    public void setItem(String item) {
        this.item = item;
    }

    public String getItemType() {
        return itemType;
    }

    public void setItemType(String itemType) {
        this.itemType = itemType;
    }

    public String getAction() {
        return action;
    }

    public void setAction(String action) {
        this.action = action;
    }

    public String getTraceId() {
        return traceId;
    }

    public void setTraceId(String traceId) {
        this.traceId = traceId;
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    @Override
    public String toString() {
        return "UserAction{" +
                "id=" + id +
                ", user='" + user + '\'' +
                ", item='" + item + '\'' +
                ", itemType='" + itemType + '\'' +
                ", action='" + action + '\'' +
                ", traceId='" + traceId + '\'' +
                ", time=" + time +
                '}';
    }
}
