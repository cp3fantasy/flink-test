package com.zz.flink.table;

import com.alibaba.fastjson.JSON;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.util.*;

public class StreamToTableTest2 {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        List<Row> rows = new ArrayList<>();
//        rows.add(Row.of("a",1,"0830"));
//        rows.add(Row.of("zhangsan",18,"M"));
//        DataStream<Row> stream = env.fromCollection(rows);
//        stream.print();
        List<Map<String,Object>> maps = new ArrayList<>();
        Map<String,Object> map = new HashMap<>();
        map.put("name","zz");
        map.put("age",18);
        maps.add(map);
        map = new HashMap<>();
        map.put("name","yy");
        map.put("age",30);
        map.put("level",1);
        maps.add(map);
        DataStream<Map<String,Object>> stream = env.fromCollection(maps);
        stream.print();
        env.execute();
//        EnvironmentSettings settings =
//                EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
//        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);
////        Schema schema = Schema.newBuilder().column.build();
//        Table table = tEnv.fromDataStream(stream);
//        String createTable = "create table pv(\n" +
//                "    pageId VARCHAR,\n" +
//                "    userId VARCHAR,\n" +
//                "    startTime BIGINT,\n" +
//                "    ts as to_timestamp(from_unixtime(startTime/1000))\n" +
//                ")with(\n" +
//                "'connector'='kafka',\n" +
//                "'topic'='pv',\n" +
//                "'scan.startup.mode'='latest-offset',\n" +
//                "'properties.bootstrap.servers'='localhost:9092',\n" +
//                "'format'='json',\n" +
//                "'properties.group.id'='flink.test.zz')\n";
//        tEnv.executeSql(createTable);
//        createTable = "create table pv_user_count(\n" +
//                "    userId VARCHAR,\n" +
//                "    cnt BIGINT\n" +
////                "    ts timestamp(3)\n" +
//                ") with (\n" +
//                "    'connector' = 'print'\n" +
//                ")";
//        tEnv.executeSql(createTable);
//        TableResult result = tEnv.executeSql("insert into pv_user_count select userId,count(1) from pv group by userId");
//
//        result.print();
    }
}
