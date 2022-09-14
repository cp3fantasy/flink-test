package com.zz.flink.table;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentInternal;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.planner.operations.PlannerQueryOperation;

import java.util.List;

public class PvTableTest {

    public static void main(String[] args) throws InterruptedException {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Configuration config = new Configuration();
        config.setInteger(RestOptions.PORT, 7100);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);
        EnvironmentSettings settings =
                EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);
        String createTable = "create table pv(\n" +
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
//        createTable = "create table pv_user_count(\n" +
//                "    userId VARCHAR,\n" +
//                "    cnt BIGINT\n" +
////                "    ts timestamp(3)\n" +
//                ") with (\n" +
//                "    'connector' = 'print'\n" +
//                ")";
//        tEnv.executeSql(createTable);
//        String sql = "insert into pv_user_count select userId,count(1) from pv group by userId";
//        String sql = "select userId,count(1) from pv group by userId";
////        TableResult result = tEnv.executeSql(sql);
//        List<Operation> operations = ((TableEnvironmentInternal) tEnv).getParser().parse(sql);
////        result.print();
////        System.out.println(operations);
//        PlannerQueryOperation operation = (PlannerQueryOperation) operations.get(0);
//        ResolvedSchema schema = operation.getResolvedSchema();
//        for (Column column : schema.getColumns()) {
//            System.out.println(column.getName() + "  " + column.getDataType());
//        }
    }
}
