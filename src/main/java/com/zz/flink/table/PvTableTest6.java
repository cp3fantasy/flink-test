package com.zz.flink.table;

import com.zz.flink.table.udf.GetGenderTableFunc;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class PvTableTest6 {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings =
                EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);
        tEnv.createTemporarySystemFunction("getGender", GetGenderTableFunc.class);
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
        String createView = "create view view_pv as\n" +
                "select pageId,userId,gender,startTime,endTime,endTime - startTime as duration,ts,status \n" +
                "from pv, LATERAL TABLE(getGender(userId))";
        tEnv.executeSql(createView);
        createTable = "create table pv_duration(\n" +
                "    pageId VARCHAR,\n" +
                "    userId VARCHAR,\n" +
                "    gender VARCHAR,\n" +
                "    duration BIGINT, \n" +
                "    ts timestamp(3)" +
                ") with (\n" +
                "    'connector' = 'print'\n" +
                ")";
        tEnv.executeSql(createTable);
        tEnv.executeSql("insert into pv_duration " +
                "select pageId,userId,gender,duration,ts from view_pv where status='success'");
//        tEnv.executeSql("create view view_pv_fail as select pageId,userId,gender,duration,ts " +
//                "from view_pv where status='fail'");
//

        createTable = "create table fail_msg(\n" +
                "    pageId VARCHAR,\n" +
                "    userId VARCHAR,\n" +
                "    startTime BIGINT, \n" +
                "    endTime BIGINT, \n" +
                "    ts timestamp(3)" +
                ") with (\n" +
                "    'connector' = 'fail'\n" +
                ")";
        tEnv.executeSql(createTable);
        tEnv.executeSql("insert into fail_msg " +
                "select pageId,userId,startTime,endTime,ts from view_pv where status='fail'");

        //        String plan = tEnv.explainSql("insert into pv_duration_male select gender,duration from pv_duration where gender='F' or gender='M'");
//        System.out.println(plan);
    }
}
