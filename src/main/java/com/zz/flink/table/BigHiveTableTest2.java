package com.zz.flink.table;

import org.apache.flink.api.dag.Pipeline;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.client.FlinkPipelineTranslationUtil;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.delegation.Executor;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.List;

public class BigHiveTableTest2 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);
        EnvironmentSettings settings =
                EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);
        String name = "myhive";
        String defaultDatabase = "flink";
        String hiveConfDir = "/Users/zhuozhang/develop/apache-hive-2.3.5-bin/conf"; // a local path

        HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir);
        tEnv.registerCatalog("myhive", hive);
        String createTable = createBigTable("big_source", 1001);
//        System.out.println(createTable);
        tEnv.executeSql(createTable);
        tEnv.useCatalog("myhive");
        tEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        createTable = createBigHiveTable("big1001", 1001);
//        System.out.println(createTable);
        tEnv.executeSql(createTable);
        tEnv.useCatalog("default_catalog");

        tEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
        String bigSelect = createBigSelect("big_source", "myhive.flink.big1001", 1001);
//        System.out.println(bigSelect);
//        TableResult result = tEnv.executeSql(bigSelect);
        tEnv.sqlUpdate(bigSelect);
//        result.print();
        tEnv.execute("hive");
//        Class cls = TableEnvironmentImpl.class;
//        Field f = cls.getDeclaredField("execEnv");
//        f.setAccessible(true);
//        Object o = f.get(tEnv);
//        Executor executor = (Executor) o;
//        Method method = cls.getDeclaredMethod("translateAndClearBuffer");
//        method.setAccessible(true);
//        List<Transformation<?>> transformations = (List<Transformation<?>>) method.invoke(tEnv);
//        f = cls.getDeclaredField("tableConfig");
//        f.setAccessible(true);
////        TableConfig tableConfig = (TableConfig) f.get(tEnv);
//        Pipeline pipeline = executor.createPipeline(transformations, tEnv.getConfig(), "test");
//        JobGraph jobGraph = FlinkPipelineTranslationUtil
//                .getJobGraph(pipeline, tEnv.getConfig().getConfiguration(), 1);
//        System.in.read();
    }

    private static String createBigSelect(String source, String sink, int fieldCount) {
        StringBuilder sb = new StringBuilder();
        sb.append("insert into ").append(sink).append(" select ");
        for (int i = 0; i < fieldCount; i++) {
            sb.append("longlonglongfield" + i);
//            if (i != fieldCount - 1) {
//                sb.append(",");
//            }
            sb.append(",");
        }
        sb.append("'2021'");
        sb.append(" from ").append(source);
        return sb.toString();
    }

    private static String createBigTable(String table, int fieldCount) {
        StringBuilder sb = new StringBuilder();
        sb.append("create table " + table + "(\n");
        for (int i = 0; i < fieldCount; i++) {
            sb.append("longlonglongfield" + i + " VARCHAR");
            if (i != fieldCount - 1) {
                sb.append(",\n");
            } else {
                sb.append(")\n");
                StringBuilder append = sb.append("with(\n" +
                        "'connector'='kafka',\n" +
                        "'topic'='big',\n" +
                        "'scan.startup.mode'='latest-offset',\n" +
                        "'properties.bootstrap.servers'='localhost:9092',\n" +
                        "'format'='json',\n" +
                        "'properties.group.id'='flink.test.zz')");
            }
        }
        return sb.toString();
    }

    private static String createBigHiveTable(String table, int fieldCount) {
        StringBuilder sb = new StringBuilder();
        sb.append("create table if not exists " + table + "(\n");
        for (int i = 0; i < fieldCount; i++) {
            sb.append("longlonglongfield" + i + " string ");
            sb.append(",\n");
        }
        sb.append("dt string\n");
        sb.append(")\n");
        sb.append("stored as orc");
        return sb.toString();
    }
}
