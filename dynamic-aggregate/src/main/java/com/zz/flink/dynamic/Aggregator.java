package com.zz.flink.dynamic;

import java.io.Serializable;
import java.util.Map;

public interface Aggregator extends Serializable {

    void aggregate(Map<String, Object> data);

    Object getResult();

}
