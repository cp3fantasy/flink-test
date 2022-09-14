package com.zz.flink.cdc.stream;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.kafka.connect.source.SourceRecord;

public class CdcStreamToTableTest {

    public static void main(String[] args) throws Exception {
        MySqlSource<Row> mySqlSource = MySqlSource.<Row>builder()
                .hostname("127.0.0.1")
                .port(3306)
                .databaseList("flink_test") // set captured database
                .tableList("flink_test.user_info") // set captured table
                .username("flink")
                .password("flink")
                .deserializer(new RowDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
                .includeSchemaChanges(false)
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // enable checkpoint
        env.enableCheckpointing(3000);

        // set 4 parallel source tasks
        DataStreamSource<Row> stream = env
                .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
                // set 4 parallel source tasks
                .setParallelism(4);
        stream.print().setParallelism(1); // use parallelism 1 for sink to keep message ordering
//        stream.addSink(sink);
        EnvironmentSettings settings =
                EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);
        tEnv.fromDataStream(stream);
//        stream.filter().map()
        env.execute("Print MySQL Snapshot + Binlog");
    }
}
