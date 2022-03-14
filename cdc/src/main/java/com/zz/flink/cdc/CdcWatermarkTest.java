package com.zz.flink.cdc;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class CdcWatermarkTest {

    public static void main(String[] args) {
        Configuration config = new Configuration();
//        config.setInteger(RestOptions.PORT, 7100);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10000);
        env.setParallelism(1);
        EnvironmentSettings settings =
                EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);
        StatementSet statementSet = tEnv.createStatementSet();
        String createTable = "create table user_info(\n" +
                "id INT,\n" +
                "userId STRING,\n" +
                "level INT,\n" +
                "update_time TIMESTAMP(3),\n" +
                "WATERMARK FOR update_time AS update_time,\n" +
                "PRIMARY KEY(userId) NOT ENFORCED \n" +
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
        createTable = "create table user_sink(\n" +
                "id INT,\n" +
                "userId STRING,\n" +
                "level INT,\n" +
                "update_time TIMESTAMP(3)\n" +
                ")with(\n" +
                "'connector' = 'print'\n" +
                ")";
        System.out.println(createTable);
        tEnv.executeSql(createTable);
        tEnv.executeSql("insert into user_sink select id,userId,level,update_time from user_info");

        System.out.println("----------------------");

    }

}
