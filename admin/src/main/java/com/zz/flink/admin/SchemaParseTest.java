package com.zz.flink.admin;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.ddl.CreateOperation;
import org.apache.flink.table.operations.ddl.CreateTableOperation;

import java.util.List;

public class SchemaParseTest {

    public static void main(String[] args) {
//        String createTable = "create table pv_src(\n" +
//                "id INT,\n" +
//                "pageId STRING,\n" +
//                "userId STRING,\n" +
//                "startTime TIMESTAMP(3),\n" +
//                "endTime TIMESTAMP(3),\n" +
//                "PRIMARY KEY(id) NOT ENFORCED \n" +
//                ")with(\n" +
//                "'connector' = 'mysql-cdc',\n" +
//                "'hostname' = 'localhost',\n" +
//                "'port' = '3306',\n" +
//                "'username' = 'flink',\n" +
//                "'password' = 'flink',\n" +
//                "'database-name' = 'flink_test',\n" +
//                "'table-name' = 'pv',\n" +
//                "'scan.incremental.snapshot.enabled' = 'true',\n" +
//                "'scan.incremental.snapshot.chunk.size' = '10'\n" +
//                ")";
        String createTable = "create table pv_src(\n" +
                "id INT,\n" +
                "pageId STRING,\n" +
                "userId STRING,\n" +
                "startTime TIMESTAMP(3),\n" +
                "endTime TIMESTAMP(3),\n" +
                "PRIMARY KEY(id) NOT ENFORCED \n" +
                ")";
        System.out.println(createTable);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings =
                EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);
        TableEnvironmentImpl impl = (TableEnvironmentImpl) tEnv;
        List<Operation> operations = impl.getParser().parse(createTable);
        CreateTableOperation operation = (CreateTableOperation) operations.get(0);
        System.out.println(operation);
        TableSchema schema = operation.getCatalogTable().getSchema();
        System.out.println(schema);
    }
}
