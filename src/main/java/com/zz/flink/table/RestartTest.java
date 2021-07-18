package com.zz.flink.table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class RestartTest {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(60000);
        EnvironmentSettings settings =
                EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);
        String createTable = "create table pv(\n" +
                "    seqNo VARCHAR,\n" +
                "    pageId VARCHAR,\n" +
                "    userId VARCHAR,\n" +
                "    startTime BIGINT,\n" +
                "    endTime BIGINT,\n" +
                "    ts as to_timestamp(from_unixtime(startTime/1000))\n" +
                ")with(\n" +
                "'connector'='kafka',\n" +
                "'topic'='pv',\n" +
                "'scan.startup.mode'='latest-offset',\n" +
                "'properties.bootstrap.servers'='localhost:9092',\n" +
                "'format'='json',\n" +
                "'properties.group.id'='flink.test.zz')\n";
        tEnv.executeSql(createTable);
        createTable = "create table user_freq(\n" +
                "    seqNo VARCHAR,\n" +
                "    userId VARCHAR,\n" +
                "    freq DOUBLE\n" +
//                "    ts timestamp(3)\n" +
                ") with (\n" +
                "    'connector' = 'print'\n" +
                ")";
        tEnv.executeSql(createTable);
        TableResult result = tEnv.executeSql("insert into user_freq select seqNo,userId,1/floor((endTime-startTime)/1000) from pv");
        result.print();
    }
}
