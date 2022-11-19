package com.zz.flink.dynamic.var;

public class LastVar extends StatVar{

    private String targetVar;

    private int lastCount;

    public String getTargetVar() {
        return targetVar;
    }

    public void setTargetVar(String targetVar) {
        this.targetVar = targetVar;
    }

    public int getLastCount() {
        return lastCount;
    }

    public void setLastCount(int lastCount) {
        this.lastCount = lastCount;
    }
}
