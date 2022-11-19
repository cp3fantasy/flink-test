package com.zz.flink.dynamic.var2;

import com.zz.flink.dynamic.SlideWindow;
import com.zz.flink.dynamic.TumbleWindow;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class VarManager {

    private volatile Map<String, StatVar> statVarMap;

    private volatile Map<String, List<StatVar>> statVarMapByGroupKey;

    public VarManager() {
        Map<String, StatVar> statVarMap = new HashMap<>();
        Map<String, List<StatVar>> statVarMapByGroupKey = new HashMap<>();
        CountVar var1 = new CountVar();
        var1.setName("count_by_page_10s");
        var1.setGroupKey("pageId");
        var1.setWindow(new TumbleWindow(10, TimeUnit.SECONDS));
        addVar(var1,statVarMap,statVarMapByGroupKey);
        SumVar var2 = new SumVar();
        var2.setName("duration_sum_by_page_10s");
        var2.setGroupKey("pageId");
        var2.setWindow(new TumbleWindow(10, TimeUnit.SECONDS));
        var2.setTargetVar("duration");
        addVar(var2,statVarMap,statVarMapByGroupKey);
        CountVar var3 = new CountVar();
        var3.setName("count_by_user_30s_10s");
        var3.setGroupKey("userId");
        var3.setFilter("duration>100");
        var3.setWindow(new SlideWindow(30, 10, TimeUnit.SECONDS));
        addVar(var3,statVarMap,statVarMapByGroupKey);
        LastVar var4 = new LastVar();
        var4.setName("last3pages_by_user_30s_10s");
        var4.setGroupKey("userId");
        var4.setWindow(new SlideWindow(30, 10, TimeUnit.SECONDS));
        var4.setTargetVar("pageId");
        var4.setLastCount(3);
        addVar(var4,statVarMap,statVarMapByGroupKey);
        this.statVarMap = statVarMap;
        this.statVarMapByGroupKey = statVarMapByGroupKey;
    }

    private void addVar(StatVar var, Map<String, StatVar> statVarMap, Map<String, List<StatVar>> statVarMapByGroupKey) {
        statVarMap.put(var.getName(),var);
        statVarMapByGroupKey.computeIfAbsent(var.getGroupKey(), s -> new ArrayList<>()).add(var);
    }


    public Map<String, StatVar> getStatVarMap() {
        return statVarMap;
    }

    public Map<String, List<StatVar>> getStatVarMapByGroupKey() {
        return statVarMapByGroupKey;
    }

    public StatVar getStatVar(String name){
        return statVarMap.get(name);
    }
}
