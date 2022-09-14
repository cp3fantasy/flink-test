package com.zz.flink.admin;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.api.internal.TableEnvironmentInternal;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.operations.ModifyOperation;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.planner.operations.PlannerQueryOperation;
import org.apache.flink.table.planner.utils.ExecutorUtils;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Collections;
import java.util.List;

public class ExecutionPlanGenerator {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings =
                EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

//        URL url = new URL("file:/users/zhuozhang/projects/flink-test/udf/target/udf-1.0.jar");
//        URLClassLoader classLoader = new URLClassLoader(new URL[]{url});
//
        ClassLoader connectorClassLoader = new URLClassLoader(new URL[]{
                new URL("file:/users/zhuozhang/projects/flink-test/lib/flink-connector-kafka_2.12-1.13.2.jar"),
                new URL("file:/users/zhuozhang/projects/flink-test/lib/kafka-clients-2.4.1.jar"),
                new URL("file:/users/zhuozhang/projects/flink-test/lib/udf-1.0.jar")},
                Thread.currentThread().getContextClassLoader());
        Class clazz = connectorClassLoader.loadClass("com.zz.flink.udf.GetGenderFunc");
        System.out.println(clazz);
        tEnv.createTemporarySystemFunction("getGender", clazz);
        Thread.currentThread().setContextClassLoader(connectorClassLoader);
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
        createTable = "create table duration_stat(\n" +
                "    gender VARCHAR,\n" +
                "    duration BIGINT \n" +
//                "    ts timestamp(3)\n" +
                ") with (\n" +
                "    'connector' = 'print'\n" +
                ")";
        System.out.println(createTable);
        tEnv.executeSql(createTable);
        String sql = "insert into duration_stat select gender,sum(duration) from pv_duration group by gender";
//        TableResult result = tEnv.executeSql(sql);
        TableEnvironmentImpl impl = (TableEnvironmentImpl) tEnv;
        List<Operation> operations = impl.getParser().parse(sql);
        ModifyOperation operation = (ModifyOperation) operations.get(0);
        List<Transformation<?>> transformations = impl.getPlanner().translate(Collections.singletonList(operation));
        StreamGraph streamGraph =
                ExecutorUtils.generateStreamGraph(env, transformations);
        System.out.println(streamGraph.getStreamingPlanAsJSON());
//        internalEnv.getParser()
    }
}
