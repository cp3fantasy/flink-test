package com.zz.flink.admin;

import org.apache.flink.configuration.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.yarn.configuration.YarnConfigOptions;

import java.net.URL;
import java.net.URLClassLoader;

public class YarnPerJobSubmit {

    public static void main(String[] args) throws Exception {
        Configuration config = new Configuration();
        config.setString("execution.target", "yarn-per-job");
        config.set(JobManagerOptions.TOTAL_PROCESS_MEMORY, MemorySize.parse("1G"));
        config.set(TaskManagerOptions.TOTAL_PROCESS_MEMORY, MemorySize.parse("1G"));
        config.set(YarnConfigOptions.FLINK_DIST_JAR,"/users/zhuozhang/develop/flink/flink-1.13.2/lib/flink-dist_2.12-1.13.2.jar");
        StreamExecutionEnvironment env = new StreamExecutionEnvironment(config);
        EnvironmentSettings settings =
                EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);
        ClassLoader connectorClassLoader = new URLClassLoader(new URL[]{
                new URL("file:/users/zhuozhang/projects/flink-test/lib/flink-connector-kafka_2.12-1.13.2.jar"),
                new URL("file:/users/zhuozhang/projects/flink-test/lib/kafka-clients-2.4.1.jar")},
                Thread.currentThread().getContextClassLoader());
        Thread.currentThread().setContextClassLoader(connectorClassLoader);
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
        System.out.println(createTable);
        tEnv.executeSql(createTable);
        createTable = "create table pv_user_count(\n" +
                "    userId VARCHAR,\n" +
                "    cnt BIGINT\n" +
//                "    ts timestamp(3)\n" +
                ") with (\n" +
                "    'connector' = 'print'\n" +
                ")";
        System.out.println(createTable);
        tEnv.executeSql(createTable);
        String sql = "insert into pv_user_count select userId,count(1) from pv group by userId";
        System.out.println(sql);
        TableResult result = tEnv.executeSql(sql);
        System.out.println(result);
    }

}
