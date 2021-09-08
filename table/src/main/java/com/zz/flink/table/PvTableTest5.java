package com.zz.flink.table;

import com.zz.flink.table.udf.GetGenderFunc;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class PvTableTest5 {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings =
                EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);
        tEnv.createTemporarySystemFunction("getGender", GetGenderFunc.class);
        String createTable = "create table pv(\n" +
                "    pageId VARCHAR,\n" +
                "    userId VARCHAR,\n" +
                "    startTime BIGINT,\n" +
                "    endTime BIGINT,\n" +
                "    ts as to_timestamp(from_unixtime(startTime))\n" +
                ")with(\n" +
                "'connector'='kafka',\n" +
                "'topic'='pv',\n" +
                "'scan.startup.mode'='latest-offset',\n" +
                "'properties.bootstrap.servers'='localhost:9092',\n" +
                "'format'='json',\n" +
                "'properties.group.id'='flink.test.zz')\n";
        tEnv.executeSql(createTable);
        String createView = "create view pv_duration as\n" +
                "select pageId,getGender(userId) as gender,ts,endTime - startTime as duration\n" +
                "from pv";
        tEnv.executeSql(createView);
        createTable = "create table pv_duration_male(\n" +
                "    gender VARCHAR,\n" +
                "    duration BIGINT \n" +
//                "    ts timestamp(3)\n" +
                ") with (\n" +
                "    'connector' = 'print'\n" +
                ")";
        tEnv.executeSql(createTable);
        TableResult result = tEnv.executeSql("insert into pv_duration_male select gender,duration from pv_duration where gender='F' or gender='M'");
        String plan = tEnv.explainSql("insert into pv_duration_male select gender,duration from pv_duration where gender='F' or gender='M'");
        System.out.println(plan);
    }
}
