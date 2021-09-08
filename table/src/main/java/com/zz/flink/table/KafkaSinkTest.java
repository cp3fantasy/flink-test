package com.zz.flink.table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class KafkaSinkTest {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings =
                EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);
        String createTable = "create table pv(\n" +
                "    pageId VARCHAR,\n" +
                "    userId VARCHAR,\n" +
                "    startTime BIGINT,\n" +
                "    ts as to_timestamp(from_unixtime(startTime/1000))\n" +
                ")with(\n" +
                "'connector'='kafka',\n" +
                "'topic'='pv',\n" +
                "'scan.startup.mode'='latest-offset',\n" +
                "'properties.bootstrap.servers'='localhost:9092',\n" +
                "'format'='json',\n" +
                "'properties.group.id'='flink.test.zz')\n";
        tEnv.executeSql(createTable);
        String createView = "create view page_user_count as\n" +
                "select pageId,userId,count(1) as cnt from pv group by pageId,userId";
        tEnv.executeSql(createView);
        createTable = "create table page_count(\n" +
                "    pageId VARCHAR,\n" +
                "    cnt BIGINT\n" +
                ") with (\n" +
                "    'connector' = 'print'\n" +
                ")";
        tEnv.executeSql(createTable);
        TableResult result = tEnv.executeSql("insert into page_count select pageId,count(1) from page_user_count group by pageId");
        result.print();
        createTable = "create table user_count(\n" +
                "    userId VARCHAR,\n" +
                "    cnt BIGINT\n" +
                ") with (\n" +
                "    'connector' = 'print'\n" +
                ")";
        tEnv.executeSql(createTable);
        result = tEnv.executeSql("insert into user_count select userId,count(1) from page_user_count group by userId");
        result.print();
    }
}
