package com.zz.flink.table.udf;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.util.*;

public class JsonUdtf1 extends TableFunction<Row> {

    public void eval(String str) {
        JSONObject jsonObject = JSON.parseObject(str);
        JSONArray array = jsonObject.getJSONArray("array");
        for (int i = 0; i < array.size(); i++) {
            JSONObject object = array.getJSONObject(i);
            for (Map.Entry<String, Object> entry : object.entrySet()) {
                System.out.println(entry.getKey() + "-->" + entry.getValue());
            }
        }
    }

    public static void main(String[] args) {
        List<Map> list = new ArrayList<>();
        Map<String,Object> map = new HashMap<>();
        map.put("x",1);
        map.put("y","2");
        Map<String,Object> varMap = new HashMap<>();
        varMap.put("v1","100");
        varMap.put("v2",200);
        map.put("vars",varMap);
        list.add(map);
        String json = JSON.toJSONString(Collections.singletonMap("array",list));
        System.out.println(json);
        new JsonUdtf1().eval(json);
    }


}
