package com.zz.flink.cdc;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class VersionedTableTest2 {

    public static void main(String[] args) {
        Configuration config = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);
        env.enableCheckpointing(10000);
        env.setParallelism(2);
        EnvironmentSettings settings =
                EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);
        String createTable = "create table user_info(\n" +
                "id INT,\n" +
                "userId STRING,\n" +
                "level INT,\n" +
                "update_time TIMESTAMP(3),\n" +
                "PRIMARY KEY(userId) NOT ENFORCED, \n" +
                "WATERMARK FOR update_time AS update_time\n"+
                ")with(\n" +
                "'connector' = 'mysql-cdc',\n" +
                "'hostname' = 'localhost',\n" +
                "'port' = '3306',\n" +
                "'username' = 'flink',\n" +
                "'password' = 'flink',\n" +
                "'database-name' = 'flink_test',\n" +
                "'table-name' = 'user_info',\n" +
                "'scan.incremental.snapshot.enabled' = 'true',\n" +
                "'scan.incremental.snapshot.chunk.size' = '10'\n" +
                ")";
        System.out.println(createTable);
        tEnv.executeSql(createTable);
        createTable = "create table pv(\n" +
                "    pageId STRING,\n" +
                "    userId STRING,\n" +
                "    startTime BIGINT,\n" +
                "    ts as to_timestamp(from_unixtime(startTime/1000)),\n" +
                "    WATERMARK FOR ts AS ts\n"+
                ")with(\n" +
                "'connector'='kafka',\n" +
                "'topic'='pv',\n" +
                "'scan.startup.mode'='latest-offset',\n" +
                "'properties.bootstrap.servers'='localhost:9092',\n" +
                "'format'='json',\n" +
                "'properties.group.id'='flink.test.zz')\n";
        System.out.println(createTable);
        tEnv.executeSql(createTable);
        createTable = "create table pv_user(\n" +
                "    pageId STRING,\n" +
                "    userId STRING,\n" +
                "    level INT,\n" +
                "    ts TIMESTAMP(3)\n" +
                ")with(\n" +
                " 'connector' = 'print'\n" +
                ")";
        System.out.println(createTable);
        tEnv.executeSql(createTable);

        StatementSet set = tEnv.createStatementSet();
        String select = "insert into pv_user \n" +
                "select pageId,pv.userId,level,ts \n" +
                "from pv left join user_info \n" +
                "FOR SYSTEM_TIME AS OF pv.ts \n" +
                "on pv.userId=user_info.userId";
        System.out.println(select);
        set.addInsertSql(select);
//        TableResult result = tEnv.executeSql(select);
//        result.print();

        createTable = "create table pv_print(\n" +
                "    pageId VARCHAR,\n" +
                "    userId VARCHAR,\n" +
                "    startTime BIGINT,\n" +
                "    ts TIMESTAMP(3)\n" +
                ")with(\n" +
                "'connector'='print')\n";
        System.out.println(createTable);
        tEnv.executeSql(createTable);
        select = "insert into pv_print select pageId,userId,startTime,ts from pv";
        System.out.println(select);
        set.addInsertSql(select);
        TableResult result = set.execute();
        result.print();
    }

}
