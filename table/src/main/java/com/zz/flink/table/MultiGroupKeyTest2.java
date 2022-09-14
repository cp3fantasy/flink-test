package com.zz.flink.table;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class MultiGroupKeyTest2 {

    public static void main(String[] args) {
        Configuration config = new Configuration();
        config.setInteger(RestOptions.PORT, 8801);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);
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
        createTable = "create table pv_count(\n" +
                "    type VARCHAR,\n" +
                "    key VARCHAR,\n" +
                "    cnt BIGINT\n" +
//                "    ts timestamp(3)\n" +
                ") with (\n" +
                "    'connector' = 'print'\n" +
                ")";
        tEnv.executeSql(createTable);
        String createView = "create view pv_key_map\n" +
                " as select 'pageId' as type,pageId as key from pv";
//        String createView = "create view pv_key_map\n" +
//                " as select 'pageId' as type,pageId as key from pv \n" +
//                "union select 'userId' as type,userId as key from pv";
        tEnv.executeSql(createView);
//        tEnv.executeSql("insert into pv_count select type,key,count(1) from pv_key_map group by type,key");
        String select = "insert into pv_count select type,key,count(1) \n" +
                "from (select 'pageId' as type,pageId as key from pv) \n" +
//                " union select 'userId' as type,userId as key from pv) \n" +
                "group by type,key";
        System.out.println(select);
        tEnv.executeSql(select);
    }
}
