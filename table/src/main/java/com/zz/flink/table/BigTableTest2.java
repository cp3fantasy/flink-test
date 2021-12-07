package com.zz.flink.table;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BigTableTest2 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);
        EnvironmentSettings settings =
                EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);
        int fieldCount = 5000;
        List<String> list = buildMsgList(fieldCount);
        DataStreamSource<String> source = env.fromCollection(list);
        SingleOutputStreamOperator<Row> stream = source.map(new MapFunction<String, Row>() {
            @Override
            public Row map(String value) throws Exception {
                Row row = new Row(RowKind.INSERT, fieldCount + 1);
                return row;
            }
        });
        Table table = tEnv.fromDataStream(stream);
        System.out.println(table.getResolvedSchema());
//        String createTable = createBigSourceTable("big_source", fieldCount);
//        System.out.println(createTable);
//        tEnv.executeSql(createTable);
//        createTable = createBigSinkTable("big_sink", fieldCount);
//        System.out.println(createTable);
//        tEnv.executeSql(createTable);
//        tEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
//        String bigSelect = createBigSelect("big_source", "big_sink", fieldCount);
//        System.out.println(bigSelect);
//        TableResult result = tEnv.executeSql(bigSelect);
//        result.print();

    }

    private static List<String> buildMsgList(int fieldCount) {
        List<String> list = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            list.add(buildMsg(fieldCount));
        }
        return list;
    }

    private static String buildMsg(int fieldCount) {
        Map<String, Object> map = new HashMap<>();
        for (int i = 0; i < fieldCount; i++) {
            map.put("f" + i, String.valueOf(i));
        }
        map.put("millis", System.currentTimeMillis());
        return JSON.toJSONString(map);
    }

    private static String createBigSelect(String source, String sink, int fieldCount) {
        StringBuilder sb = new StringBuilder();
        sb.append("insert into ").append(sink).append(" select ");
        for (int i = 0; i < fieldCount; i++) {
            sb.append("f" + i);
            sb.append(",");
        }
        sb.append("millis ");
        sb.append(" from ").append(source);
        return sb.toString();
    }

    private static String createBigSourceTable(String table, int fieldCount) {
        StringBuilder sb = new StringBuilder();
        sb.append("create table " + table + "(\n");
        for (int i = 0; i < fieldCount; i++) {
            sb.append("f" + i + " VARCHAR,\n");
        }
        sb.append("millis BIGINT)\n");
        sb.append("with(\n" +
                "'connector'='kafka',\n" +
                "'topic'='big',\n" +
                "'scan.startup.mode'='latest-offset',\n" +
                "'properties.bootstrap.servers'='localhost:9092',\n" +
                "'format'='json',\n" +
                "'properties.group.id'='flink.test.zz')");
        return sb.toString();
    }

    private static String createBigSinkTable(String table, int fieldCount) {
        StringBuilder sb = new StringBuilder();
        sb.append("create table " + table + "(\n");
        for (int i = 0; i < fieldCount; i++) {
            sb.append("f" + i + " VARCHAR,");
        }
        sb.append("seconds BIGINT)\n");
        sb.append("with(\n" +
                "'connector'='print')");
        return sb.toString();
    }

}
