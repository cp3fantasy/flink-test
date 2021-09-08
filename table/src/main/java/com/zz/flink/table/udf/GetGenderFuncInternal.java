package com.zz.flink.table.udf;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.functions.ScalarFunction;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class GetGenderFuncInternal extends ScalarFunction {

    @DataTypeHint("Map<String,String>")
    public Map<String, String> eval(String key) {
        System.out.println("eval "+key);
//        if (key.hashCode()%3 > 1) {
        if (new Random().nextInt(3) > 0) {
            String gender = key.hashCode() % 2 == 0 ? "M" : "F";
            Map<String, String> map = new HashMap<>();
            map.put("status", "success");
            map.put("gender", gender);
            return map;
        } else {
            return Collections.singletonMap("status", "fail");
        }
    }
}
