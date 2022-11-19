package com.zz.flink.dynamic.rule;

import java.io.Serializable;
import java.util.Map;

public interface Aggregator extends Serializable {

    void aggregate(Map<String, Object> data);

    Object getResult();

}
