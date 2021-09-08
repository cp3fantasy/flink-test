package com.zz.flink.common.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zz.flink.common.model.PageView;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class PageViewSerializer implements Serializer<PageView> {

    private  ObjectMapper mapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public byte[] serialize(String topic, PageView pageView) {
        try {
            return mapper.writeValueAsBytes(pageView);
        } catch (JsonProcessingException e) {
            return null;
        }
    }

    @Override
    public void close() {

    }
}
