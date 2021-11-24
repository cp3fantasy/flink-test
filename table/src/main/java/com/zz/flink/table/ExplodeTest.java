package com.zz.flink.table;

import com.zz.flink.table.udf.ExplodeStringListFunc;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class ExplodeTest {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings =
                EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);
        tEnv.createTemporarySystemFunction("explodeStrings", ExplodeStringListFunc.class);
        String createTable = "create table pv_batch(\n" +
                "    userId VARCHAR,\n" +
                "    pageIds ARRAY<VARCHAR>,\n" +
                "    startTime BIGINT,\n" +
                "    ts as to_timestamp(from_unixtime(startTime))\n" +
                ")with(\n" +
                "'connector'='kafka',\n" +
                "'topic'='pv_batch',\n" +
                "'scan.startup.mode'='latest-offset',\n" +
                "'properties.bootstrap.servers'='localhost:9092',\n" +
                "'format'='json',\n" +
                "'properties.group.id'='flink.test.zz')\n";
        tEnv.executeSql(createTable);
        createTable = "create table pv(\n" +
                "    userId VARCHAR,\n" +
                "    pageId VARCHAR \n" +
//                "    ts timestamp(3)\n" +
                ") with (\n" +
                "    'connector' = 'print'\n" +
                ")";
        tEnv.executeSql(createTable);
        TableResult result = tEnv.executeSql("insert into pv select userId,s as pageId from pv_batch,LATERAL TABLE(explodeStrings(pageIds))");

        result.print();
    }
}
