package com.zz.flink.table.udf;

import org.apache.flink.table.functions.ScalarFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

public class RandomErrorFunc extends ScalarFunction {

    private static Logger logger = LoggerFactory.getLogger(RandomErrorFunc.class);

    public String eval(String key) {
        if (new Random().nextInt(100) == 0) {
            logger.error("eval error",new RuntimeException());
            throw new RuntimeException("random error");
        } else {
            return key;
        }
    }
}
