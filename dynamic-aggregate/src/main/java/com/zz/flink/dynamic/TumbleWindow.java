package com.zz.flink.dynamic;

public class TumbleWindow extends TimeWindow{

    private int size;

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }
}
