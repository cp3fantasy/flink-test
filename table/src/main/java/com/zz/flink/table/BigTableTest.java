package com.zz.flink.table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class BigTableTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);
        EnvironmentSettings settings =
                EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);
        int fieldCount = 5000;
        String createTable = createBigSourceTable("big_source", fieldCount);
        System.out.println(createTable);
        tEnv.executeSql(createTable);
        createTable = createBigSinkTable("big_sink", fieldCount);
        System.out.println(createTable);
        tEnv.executeSql(createTable);
        tEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
        String bigSelect = createBigSelect("big_source", "big_sink", fieldCount);
        System.out.println(bigSelect);
        TableResult result = tEnv.executeSql(bigSelect);
        result.print();

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
