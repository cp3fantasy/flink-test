package com.zz.flink.debezium;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class CdcTest2 {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings =
                EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);
        String createTable = "CREATE TABLE cdc_src (\n" +
                "  before MAP<STRING,STRING>,\n" +
                "  after MAP<STRING,STRING>,\n" +
                "  op STRING,\n" +
                "  table_name STRING\n" +
                ") WITH (\n" +
                " 'connector' = 'kafka',\n" +
                " 'topic' = 'products_binlog',\n" +
                " 'properties.bootstrap.servers' = 'localhost:9092',\n" +
                " 'properties.group.id' = 'testGroup',\n" +
                " -- using 'debezium-json' as the format to interpret Debezium JSON messages\n" +
                " -- please use 'debezium-avro-confluent' if Debezium encodes messages in Avro format\n" +
                " 'format' = 'cdc-json'\n" +
                ")";
        tEnv.executeSql(createTable);
        createTable = "create table cdc_sink(\n" +
                "  before MAP<STRING,STRING>,\n" +
                "  after MAP<STRING,STRING>,\n" +
                "  op STRING,\n" +
                "  table_name STRING\n" +
                ") with (\n" +
                "    'connector' = 'print'\n" +
                ")";
        tEnv.executeSql(createTable);
        TableResult result = tEnv.executeSql("insert into cdc_sink select before,after,op,table_name from cdc_src");
        result.print();
    }
}
