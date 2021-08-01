package com.zz.flink.table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

public class PartitionedHiveTableTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(30000);
        EnvironmentSettings settings =
                EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);
        String name = "myhive";
        String defaultDatabase = "flink";
        String hiveConfDir = "/Users/zhuozhang/develop/apache-hive-2.3.5-bin/conf"; // a local path
        HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir);
        tEnv.registerCatalog("myhive", hive);
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
        tEnv.useCatalog("myhive");
        tEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        createTable  = "create table pv2(\n" +
                "    pageId VARCHAR(20),\n" +
                "    userId VARCHAR(20),\n" +
                "    startTime BIGINT,\n" +
                "    dt STRING\n" +
                ") PARTITIONED BY (dt STRING) " +
                "stored as orc TBLPROPERTIES(\n" +
                "'sink.partition-commit.policy.trigger'='process-time',\n"+
                "'sink.partition-commit.policy.kind'='metastore,success-file',\n"+
                "'sink.partition-commit.delay'='1 min')";
        tEnv.executeSql(createTable);
        tEnv.useCatalog("default_catalog");
        tEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
        TableResult result = tEnv.executeSql("insert into myhive.flink.pv2 select pageId,userId,startTime,DATE_FORMAT(ts,'yyyy-MM-dd') from pv_src");

        result.print();

    }

}
