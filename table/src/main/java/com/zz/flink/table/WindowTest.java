package com.zz.flink.table;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class WindowTest {


    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings =
                EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);
        // access flink configuration
        Configuration configuration = tEnv.getConfig().getConfiguration();
// set low-level key-value options
        configuration.setString("table.exec.mini-batch.enabled", "true"); // enable mini-batch optimization
        configuration.setString("table.exec.mini-batch.allow-latency", "30s"); // use 5 seconds to buffer input records
        configuration.setString("table.exec.mini-batch.size", "5000"); // the maximum number of records can be buffered by each aggregate operator task
        String createTable = "create table pv(\n" +
                "    pageId VARCHAR,\n" +
                "    userId VARCHAR,\n" +
                "    startTime BIGINT,\n" +
                "    ts as to_timestamp(from_unixtime(startTime/1000)),\n" +
                "    ptime AS PROCTIME()\n" +
                ")with(\n" +
                "'connector'='kafka',\n" +
                "'topic'='pv',\n" +
                "'scan.startup.mode'='latest-offset',\n" +
                "'properties.bootstrap.servers'='localhost:9092',\n" +
                "'format'='json',\n" +
                "'properties.group.id'='flink.test.zz')\n";
        tEnv.executeSql(createTable);
        createTable = "create table pv_user_count(\n" +
                "    userId VARCHAR,\n" +
                "    `minute` BIGINT,\n" +
                "    cnt BIGINT\n" +
                ") with (\n" +
                "    'connector' = 'print'\n" +
                ")";
        tEnv.executeSql(createTable);
        TableResult result = tEnv.executeSql("insert into pv_user_count " +
                "select userId,MINUTE(ts),count(1) from pv group by userId,MINUTE(ts)");
        result.print();
    }
}
