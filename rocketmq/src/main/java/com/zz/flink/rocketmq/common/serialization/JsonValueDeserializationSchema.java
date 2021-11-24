package com.zz.flink.rocketmq.common.serialization;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.typeinfo.TypeInformation;

public class JsonValueDeserializationSchema<T> implements KeyValueDeserializationSchema<T> {

    private Class<T> clazz;

    @Override
    public T deserializeKeyAndValue(byte[] key, byte[] value) {
        return JSON.parseObject(value,clazz);
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return TypeInformation.of(clazz);
    }

}
