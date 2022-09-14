package com.zz.flink.cdc.stream;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RecordStreamToTableTest2 {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        List<Row> rows = new ArrayList<>();
//        rows.add(Row.of("a",1,"0830"));
//        rows.add(Row.of("zhangsan",18,"M"));
//        DataStream<Row> stream = env.fromCollection(rows);
//        stream.print();
        List<Record> records = new ArrayList<>();
        Record record = new Record();
        record.setTable("user");
        Map<String,Object> map = new HashMap<>();
        map.put("name","zz");
        map.put("age",18);
        record.setData(map);
        record.setRowKind(RowKind.INSERT);
        records.add(record);
        record = new Record();
        record.setTable("pv");
        map = new HashMap<>();
        map.put("name","zz");
        map.put("page","page1");
        record.setData(map);
        record.setRowKind(RowKind.INSERT);
        records.add(record);
        DataStream<Record> stream = env.fromCollection(records);
        stream.print();
        EnvironmentSettings settings =
                EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);
        DataStream<Row> userStream = stream.filter(new FilterFunction<Record>() {
            @Override
            public boolean filter(Record record) throws Exception {
                return record.getTable().equals("user");
            }
        }).map(new MapFunction<Record, Row>() {
            @Override
            public Row map(Record record) throws Exception {
                Row row = Row.withNames();
                for(Map.Entry<String, Object> entry:record.getData().entrySet()){
                    row.setField(entry.getKey(),entry.getValue());
                }
                row.setKind(record.getRowKind());
                return row;
            }
        }, new RowTypeInfo(new TypeInformation[]{TypeInformation.of(String.class),TypeInformation.of(int.class)},
                new String[]{"name","age"}));
        Table userTable = tEnv.fromChangelogStream(userStream);
        tEnv.createTemporaryView("user_src",userTable);
        String createTable = "create table user_sink(\n" +
                "    name string,\n" +
                "    age int\n" +
                ")with(\n" +
                "'connector'='print'\n" +
                ")\n";
        tEnv.executeSql(createTable);
        tEnv.executeSql("insert into user_sink select name,age from user_src");
        env.execute();
//
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
//
//
//        result.print();
    }
}
