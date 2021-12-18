package com.zz.flink.cdc;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class UserCdcFilterTest {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10000);
        env.setParallelism(2);
        EnvironmentSettings settings =
                EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);
        String createTable = "create table user_src(\n" +
                "id INT,\n" +
                "userId STRING,\n" +
                "level INT,\n" +
                "amount DECIMAL(38,10),\n" +
                "update_time TIMESTAMP(3),\n" +
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
        createTable = "create table user_info(\n" +
                "id INT,\n" +
                "userId STRING,\n" +
                "level INT,\n" +
                "amount DECIMAL(38,10),\n" +
                "update_time TIMESTAMP(3)\n" +
                ")with(\n" +
                " 'connector' = 'print'\n" +
                ")";
        System.out.println(createTable);
        tEnv.executeSql(createTable);
        TableResult result = tEnv.executeSql("insert into user_info select id,userId,level,amount,update_time from user_src where level>1");
        result.print();
    }
}
