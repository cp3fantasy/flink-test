package com.zz.flink.table;

import com.zz.flink.table.udf.GetGenderFuncInternal;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class PvTableTest4 {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings =
                EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);
        tEnv.createTemporarySystemFunction("getGender", GetGenderFuncInternal.class);
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
        String createView = "create view pv_duration_internal as\n" +
                "select pageId,getGender(userId) as gender_result,endTime - startTime as duration,ts\n" +
                "from pv";
        tEnv.executeSql(createView);
        createTable = "create table pv_duration(\n" +
                "    pageId VARCHAR,\n" +
                "    gender VARCHAR,\n" +
                "    duration BIGINT, \n" +
                "    status VARCHAR, \n" +
                "    ts timestamp(3)\n" +
                ") with (\n" +
                "    'connector' = 'print'\n" +
                ")";

        tEnv.executeSql(createTable);

        tEnv.executeSql("create view view1 as  select pageId, gender_result['gender'] as gender,duration,gender_result['status'] as status, ts from pv_duration_internal");

        TableResult result = tEnv.executeSql("insert into pv_duration select pageId, gender,duration,status, ts from view1 where status='success'");

//        result.print();
    }
}
