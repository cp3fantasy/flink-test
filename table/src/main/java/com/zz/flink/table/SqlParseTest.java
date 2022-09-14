package com.zz.flink.table;

import com.zz.flink.table.udf.GetGenderFunc;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentInternal;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.planner.operations.PlannerQueryOperation;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.List;

public class SqlParseTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings =
                EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        URL url = new URL("file:/users/zhuozhang/projects/flink-test/udf/target/udf-1.0.jar");
        URLClassLoader classLoader = new URLClassLoader(new URL[]{url});
        Class clazz = classLoader.loadClass("com.zz.flink.udf.GetGenderFunc");
//        System.out.println(clazz);
        tEnv.createTemporarySystemFunction("getGender", clazz);
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
        String createView = "create view pv_duration as\n" +
                "select pageId,getGender(userId) as gender,ts,endTime - startTime as duration\n" +
                "from pv";
        tEnv.executeSql(createView);
        String sql = "select gender,sum(duration) as total_duration from pv_duration group by gender";
//        TableResult result = tEnv.executeSql(sql);
        List<Operation> operations = ((TableEnvironmentInternal) tEnv).getParser().parse(sql);
//        result.print();
//        System.out.println(operations);
        PlannerQueryOperation operation = (PlannerQueryOperation) operations.get(0);
        ResolvedSchema schema = operation.getResolvedSchema();
        for (Column column : schema.getColumns()) {
            System.out.println(column.getName() + "  " + column.getDataType());
        }
    }
}
