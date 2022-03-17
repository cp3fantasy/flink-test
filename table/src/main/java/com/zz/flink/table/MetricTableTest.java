package com.zz.flink.table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class MetricTableTest {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings =
                EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);
        String createTable = "create table metric(\n" +
                "    name VARCHAR,\n" +
                "    tags Map<String,String>,\n" +
                "    `time` TIMESTAMP(3),\n" +
                "    `value` VARCHAR\n" +
                ")with(\n" +
                "'connector'='kafka',\n" +
                "'topic'='flink_metrics',\n" +
                "'scan.startup.mode'='latest-offset',\n" +
                "'properties.bootstrap.servers'='localhost:9092',\n" +
                "'format'='json',\n" +
                "'properties.group.id'='flink.test.zz')\n";
        tEnv.executeSql(createTable);
        createTable = "create table metric_sink(\n" +
                "    name VARCHAR,\n" +
                "    tags Map<String,String>,\n" +
                "    `time` TIMESTAMP(3),\n" +
                "    `value` VARCHAR\n" +
                ") with (\n" +
                "    'connector' = 'print'\n" +
                ")";
        tEnv.executeSql(createTable);
        TableResult result = tEnv.executeSql("insert into metric_sink select name,tags,`time`,`value` from metric");

        result.print();
    }
}
