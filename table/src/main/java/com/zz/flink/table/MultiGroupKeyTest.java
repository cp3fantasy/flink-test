package com.zz.flink.table;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class MultiGroupKeyTest {

    public static void main(String[] args) {
        Configuration config = new Configuration();
        config.setInteger(RestOptions.PORT, 8800);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);
        EnvironmentSettings settings =
                EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);
        StatementSet set = tEnv.createStatementSet();
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
        createTable = "create table pv_user_count(\n" +
                "    userId VARCHAR,\n" +
                "    cnt BIGINT\n" +
//                "    ts timestamp(3)\n" +
                ") with (\n" +
                "    'connector' = 'print'\n" +
                ")";
        tEnv.executeSql(createTable);
        set.addInsertSql("insert into pv_user_count select userId,count(1) from pv group by userId");
//        tEnv.executeSql("insert into pv_user_count select userId,count(1) from pv group by userId");
        createTable = "create table pv_page_count(\n" +
                "    pageId VARCHAR,\n" +
                "    cnt BIGINT\n" +
//                "    ts timestamp(3)\n" +
                ") with (\n" +
                "    'connector' = 'print'\n" +
                ")";
        tEnv.executeSql(createTable);
        set.addInsertSql("insert into pv_page_count select pageId,count(1) from pv group by pageId");
//        tEnv.executeSql("insert into pv_page_count select pageId,count(1) from pv group by pageId");
        set.execute();
//        System.out.println(env.getExecutionPlan());
    }
}
