package com.zz.flink.iceberg;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.List;

public class IcebergTest2 {

    public static void main(String[] args) throws Exception {
        Configuration config = new Configuration();
        config.setInteger(RestOptions.PORT, 7100);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);
        env.enableCheckpointing(30000);
        EnvironmentSettings settings =
                EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);
        String createTable = "create table pv_src(\n" +
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
        createTable = "create table if not exists pv_sink(\n" +
                "    pageId VARCHAR(20),\n" +
                "    userId VARCHAR(20),\n" +
                "    startTime BIGINT\n" +
                ")with (\n" +
                "    'connector' = 'iceberg',\n" +
                " 'catalog-name' = 'test',\n"+
                "  'hive-conf-dir'='/Users/zhuozhang/develop/apache-hive-2.3.5-bin/conf',\n" +
                " 'catalog-database' = 'flink'"+
                ")";
        System.out.println(createTable);
        tEnv.executeSql(createTable);
        System.out.println("create sink ok");
        TableResult result = tEnv.executeSql("insert into pv_sink select pageId,userId,startTime from pv_src");
        result.print();
    }

}



