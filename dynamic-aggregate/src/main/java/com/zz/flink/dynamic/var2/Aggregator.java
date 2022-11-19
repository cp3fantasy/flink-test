package com.zz.flink.dynamic.var2;

import java.io.Serializable;
import java.util.Map;

public interface Aggregator extends Serializable {

    void aggregate(Map<String, Object> data, StatVar var);

    Object getResult();

}
