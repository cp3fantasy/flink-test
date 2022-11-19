package com.zz.flink.table.udf;

import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;

import java.util.Random;

public class GetGenderFunc extends ScalarFunction {

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
    }

    public String eval(String key) {
        System.out.println("eval " + key);
        return new Random().nextInt(2) == 0 ? "M" : "F";
//        return key.hashCode() % 2 == 0 ? "M" : "F";
    }
}
