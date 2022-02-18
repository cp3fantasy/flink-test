package com.zz.flink.cdc;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class UserCdcWindowAggTest {

    public static void main(String[] args) {
        Configuration conf = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.enableCheckpointing(10000);
        env.setParallelism(2);
        EnvironmentSettings settings =
                EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);
        String createTable = "create table user_src(\n" +
                "id INT,\n" +
                "userId STRING,\n" +
                "level INT,\n" +
                "amount DECIMAL(38,10),\n" +
                "update_time TIMESTAMP(3),\n" +
                "WATERMARK FOR update_time AS update_time,\n"+
                "PRIMARY KEY(userId) NOT ENFORCED \n" +
                ")with(\n" +
                "'connector' = 'mysql-cdc',\n" +
                "'hostname' = 'localhost',\n" +
                "'port' = '3306',\n" +
                "'username' = 'flink',\n" +
                "'password' = 'flink',\n" +
                "'database-name' = 'flink_test',\n" +
                "'table-name' = 'user_info',\n" +
                "'scan.incremental.snapshot.enabled' = 'true',\n" +
                "'scan.incremental.snapshot.chunk.size' = '10'\n" +
                ")";
        System.out.println(createTable);
        tEnv.executeSql(createTable);
        createTable = "create table user_stat(\n" +
                "amount DECIMAL(38,10),\n" +
                "window_start TIMESTAMP(3),\n" +
                "window_end TIMESTAMP(3),\n" +
                "level INT\n" +
                ")with(\n" +
                " 'connector' = 'print'\n" +
                ")";
        System.out.println(createTable);
        tEnv.executeSql(createTable);
        String sql ="insert into user_stat \n" +
                "select sum(amount),window_start,window_end,level \n" +
                "from TABLE(TUMBLE(TABLE user_src, DESCRIPTOR(update_time), INTERVAL '1' MINUTES))\n" +
                "GROUP BY window_start,window_end,level";
        System.out.println(sql);
        TableResult result = tEnv.executeSql(sql);
        result.print();
    }
}
