package com.zz.flink.debezium;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class CdcTest3 {

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
        createTable = "create table table0(\n" +
                "  name STRING,\n" +
                "  description STRING\n" +
                ") with (\n" +
                "    'connector' = 'print'\n" +
                ")";
        tEnv.executeSql(createTable);
        createTable = "create table table1(\n" +
                "  name STRING,\n" +
                "  description STRING\n" +
                ") with (\n" +
                "    'connector' = 'print'\n" +
                ")";
        tEnv.executeSql(createTable);
        StatementSet set = tEnv.createStatementSet();
        set.addInsertSql("insert into table0 select before['name'],before['description'] from cdc_src where table_name='table0' and (op='-U' or op='D')");
        set.addInsertSql("insert into table0 select after['name'],after['description'] from cdc_src where table_name='table0' and (op='I' or op='+U')");
        set.addInsertSql("insert into table1 select before['name'],before['description'] from cdc_src where table_name='table1' and (op='-U' or op='D')");
        set.addInsertSql("insert into table1 select after['name'],after['description'] from cdc_src where table_name='table1' and (op='I' or op='+U')");
        set.execute();
        //        TableResult result = tEnv.executeSql("insert into table1 select after['name'],after['description'] from cdc_src where table_name='table1'");
//        result.print();
    }
}
