package com.zz.flink.table;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class LargeSqlTest2 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings =
                EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);
        String prop = "with(\n" +
                "'connector'='kafka',\n" +
                "'topic'='big',\n" +
                "'scan.startup.mode'='latest-offset',\n" +
                "'properties.bootstrap.servers'='localhost:9092',\n" +
                "'format'='json',\n" +
                "'properties.group.id'='flink.test.zz')";
        int fieldCount = 10;
        String createTable = createBigTable("big_source", fieldCount, prop);
//        System.out.println(createTable);
        tEnv.executeSql(createTable);
        prop = "with(\n" +
                "'connector'='print')";
        createTable = createBigTable("big_sink", fieldCount, prop);
//        System.out.println(createTable);
        tEnv.executeSql(createTable);
        String select = createBigSelect("big_source", "big_sink", fieldCount);
//        System.out.println(select);
        tEnv.executeSql(select);
        System.out.println("ok");
    }

    private static String createBigSelect(String source, String sink, int fieldCount) {
        StringBuilder sb = new StringBuilder();
        sb.append("insert into ").append(sink).append(" select ");
        for (int i = 0; i < fieldCount; i++) {
            sb.append("longlonglongfield" + i);
            if (i != fieldCount - 1) {
                sb.append(",");
            }
        }
        sb.append(" from ").append(source);
        return sb.toString();
    }

    private static String createBigTable(String table, int fieldCount, String prop) {
        StringBuilder sb = new StringBuilder();
        sb.append("create table " + table + "(\n");
        for (int i = 0; i < fieldCount; i++) {
            sb.append("longlonglongfield" + i + " VARCHAR");
            if (i != fieldCount - 1) {
                sb.append(",\n");
            } else {
                sb.append(")\n");
                sb.append(prop);
            }
        }
        return sb.toString();
    }
}
