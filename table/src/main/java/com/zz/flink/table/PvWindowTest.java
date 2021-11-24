package com.zz.flink.table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class PvWindowTest {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings =
                EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);
        String createTable = "create table pv(\n" +
                "    pageId VARCHAR,\n" +
                "    userId VARCHAR,\n" +
                "    startTime BIGINT,\n" +
                "    ts as to_timestamp(from_unixtime(startTime/1000)),\n" +
                "   WATERMARK FOR ts AS ts - INTERVAL '5' SECOND" +
                ")with(\n" +
                "'connector'='kafka',\n" +
                "'topic'='pv',\n" +
                "'scan.startup.mode'='latest-offset',\n" +
                "'properties.bootstrap.servers'='localhost:9092',\n" +
                "'format'='json',\n" +
                "'properties.group.id'='flink.test.zz')\n";
        tEnv.executeSql(createTable);
        createTable = "create table pv_stat(\n" +
                "    pageId VARCHAR,\n" +
                "    userId VARCHAR,\n" +
                "    cnt BIGINT,\n" +
                "    ts timestamp(3)\n" +
                ") with (\n" +
//                "'connector'='print')\n";
                "'connector'='kafka',\n" +
                "'topic'='pv_stat_15s',\n" +
                "'properties.bootstrap.servers'='localhost:9092',\n" +
                "'format'='json',\n" +
                "'properties.group.id'='flink.test.zz')\n";
        tEnv.executeSql(createTable);
        String select = "insert into pv_stat\n " +
                "select pageId,userId,count(1),window_end\n " +
                "from Table(TUMBLE(Table pv,DESCRIPTOR(ts),INTERVAL '15' SECOND))\n" +
                "group by window_start,window_end,pageId,userId";
        System.out.println(select);
        TableResult result = tEnv.executeSql(select);
        result.print();
    }
}
