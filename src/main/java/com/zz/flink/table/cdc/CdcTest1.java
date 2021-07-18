package com.zz.flink.table.cdc;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class CdcTest1 {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings =
                EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);
        String createTable = "CREATE TABLE topic_products (\n" +
                "  -- schema is totally the same to the MySQL \"products\" table\n" +
                "  id BIGINT,\n" +
                "  name STRING,\n" +
                "  description STRING,\n" +
                "  weight DECIMAL(10, 2)\n" +
                ") WITH (\n" +
                " 'connector' = 'kafka',\n" +
                " 'topic' = 'products_binlog',\n" +
                " 'properties.bootstrap.servers' = 'localhost:9092',\n" +
                " 'properties.group.id' = 'testGroup',\n" +
                " -- using 'debezium-json' as the format to interpret Debezium JSON messages\n" +
                " -- please use 'debezium-avro-confluent' if Debezium encodes messages in Avro format\n" +
                " 'format' = 'debezium-json'\n" +
                ")";
        tEnv.executeSql(createTable);
        createTable = "create table topic_products_sink(\n" +
                "  id BIGINT,\n" +
                "  name STRING,\n" +
                "  description STRING,\n" +
                "  weight DECIMAL(10, 2)\n" +
                ") with (\n" +
                "    'connector' = 'print'\n" +
                ")";
        tEnv.executeSql(createTable);
        TableResult result = tEnv.executeSql("insert into topic_products_sink select id,name,description,weight from topic_products");
        result.print();
    }
}
