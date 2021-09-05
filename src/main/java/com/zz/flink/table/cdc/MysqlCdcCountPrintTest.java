package com.zz.flink.table.cdc;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class MysqlCdcCountPrintTest {

    public static void main(String[] args) {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Configuration config = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);
        env.enableCheckpointing(10000);
        EnvironmentSettings settings =
                EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);
        String createTable = "create table pv_src(\n" +
                "id INT,\n" +
                "pageId STRING,\n" +
                "userId STRING,\n" +
                "startTime TIMESTAMP(3),\n" +
                "endTime TIMESTAMP(3),\n" +
                "PRIMARY KEY(id) NOT ENFORCED \n" +
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
        createTable = "create table page_count(\n" +
                "    pageId STRING,\n" +
                "    `count` BIGINT\n" +
                ")with(\n" +
                " 'connector' = 'print'\n" +
                ")";
        System.out.println(createTable);
        tEnv.executeSql(createTable);
        TableResult result = tEnv.executeSql("insert into page_count select pageId,count(1) from pv_src group by pageId");
        result.print();
    }
}
