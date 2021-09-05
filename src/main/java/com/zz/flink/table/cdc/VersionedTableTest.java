package com.zz.flink.table.cdc;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class VersionedTableTest {

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
                "id INT,\n" +
                "pageId STRING,\n" +
                "userId STRING,\n" +
                "startTime TIMESTAMP(3),\n" +
                "endTime TIMESTAMP(3),\n" +
                "PRIMARY KEY(id) NOT ENFORCED, \n" +
                "WATERMARK FOR startTime AS startTime\n"+
                ")with(\n" +
                "'connector' = 'mysql-cdc',\n" +
                "'hostname' = 'localhost',\n" +
                "'port' = '3306',\n" +
                "'username' = 'flink',\n" +
                "'password' = 'flink',\n" +
                "'database-name' = 'flink_test',\n" +
                "'table-name' = 'pv')\n";
        System.out.println(createTable);
        tEnv.executeSql(createTable);
        createTable = "create table pv_user(\n" +
                "    pageId STRING,\n" +
                "    userId STRING,\n" +
                "    level INT,\n" +
                "    startTime TIMESTAMP(3)\n" +
                ")with(\n" +
                " 'connector' = 'print'\n" +
                ")";
        System.out.println(createTable);
        tEnv.executeSql(createTable);
        String select = "insert into pv_user \n" +
                "select pageId,pv.userId,level,startTime \n" +
                "from pv left join user_info \n" +
                "FOR SYSTEM_TIME AS OF pv.startTime \n" +
                "on pv.userId=user_info.userId";
        System.out.println(select);
        TableResult result = tEnv.executeSql(select);
        result.print();
    }

}
