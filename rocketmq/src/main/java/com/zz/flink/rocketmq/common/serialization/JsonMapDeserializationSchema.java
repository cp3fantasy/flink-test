package com.zz.flink.rocketmq.common.serialization;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.util.Map;

public class JsonMapDeserializationSchema implements KeyValueDeserializationSchema<Map> {

    private static final TypeReference<Map<String,Object>> MAP_TYPE_REFERENCE =
            new TypeReference<Map<String,Object>>(){};

    @Override
    public Map<String, Object> deserializeKeyAndValue(byte[] key, byte[] value) {
        return JSON.parseObject(new String(value),MAP_TYPE_REFERENCE);
    }

    @Override
    public TypeInformation<Map> getProducedType() {
        return TypeInformation.of(Map.class);
    }
}
