package com.zz.flink.jobs.loan;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.rocketmq.flink.RocketMQConfig;
import org.apache.rocketmq.flink.RocketMQSink;
import org.apache.rocketmq.flink.RocketMQSource;
import org.apache.rocketmq.flink.common.serialization.JsonMapDeserializationSchema;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;

import static org.apache.rocketmq.flink.RocketMQConfig.CONSUMER_OFFSET_LATEST;


public class LoanFlowControl {

    /**
     * Source Config
     *
     * @return properties
     */
    private static Properties getLoanConsumerProps() {
        Properties consumerProps = new Properties();
        consumerProps.setProperty(
                RocketMQConfig.NAME_SERVER_ADDR,
                "localhost:9876");
        consumerProps.setProperty(RocketMQConfig.CONSUMER_GROUP, "loan_flow_control");
        consumerProps.setProperty(RocketMQConfig.CONSUMER_TOPIC, "IDATA-EPLUS_LOAN_LIMIT_CHANGE");
        consumerProps.setProperty(RocketMQConfig.CONSUMER_OFFSET_RESET_TO, CONSUMER_OFFSET_LATEST);
        return consumerProps;
    }

    private static Properties getControlConsumerProps() {
        Properties consumerProps = new Properties();
        consumerProps.setProperty(
                RocketMQConfig.NAME_SERVER_ADDR,
                "localhost:9876");
        consumerProps.setProperty(RocketMQConfig.CONSUMER_GROUP, "loan_flow_control");
        consumerProps.setProperty(RocketMQConfig.CONSUMER_TOPIC, "BLOAN-RCPM_EVENT");
        consumerProps.setProperty(RocketMQConfig.CONSUMER_OFFSET_RESET_TO, CONSUMER_OFFSET_LATEST);
        return consumerProps;
    }

    /**
     * Sink Config
     *
     * @return properties
     */
    private static Properties getProducerProps() {
        Properties producerProps = new Properties();
        producerProps.setProperty(
                RocketMQConfig.NAME_SERVER_ADDR,
                "localhost:9876");
        producerProps.setProperty(RocketMQConfig.PRODUCER_GROUP, "loan_flow_control");
        return producerProps;
    }

    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);

//        // for local
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

//         for cluster
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);
//        env.setStateBackend(new RocksDBStateBackend(""));
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // start a checkpoint every 10s
        env.enableCheckpointing(10000);
        // advanced options:
        // set mode to exactly-once (this is the default)
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // checkpoints have to complete within one minute, or are discarded
//        env.getCheckpointConfig().setCheckpointTimeout(60000);
        // make sure 500 ms of progress happen between checkpoints
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        // allow only one checkpoint to be in progress at the same time
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // enable externalized checkpoints which are retained after job cancellation
//        env.getCheckpointConfig()
//                .enableExternalizedCheckpoints(
//                        CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        JsonMapDeserializationSchema schema = new JsonMapDeserializationSchema();

        DataStreamSource<Map> loanSource =
                env.addSource(new RocketMQSource<>(schema, getLoanConsumerProps()));
        DataStreamSource<Map> controlSource =
                env.addSource(new RocketMQSource<>(schema, getControlConsumerProps()));
        DataStream<Map> source = loanSource.union(controlSource);
        source.print();
        WatermarkStrategy<Map> strategy = WatermarkStrategy.<Map>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner(new SerializableTimestampAssigner<Map>() {
                    @Override
                    public long extractTimestamp(Map map, long recordTimestamp) {
                        String timestamp = (String) map.get("timestamp");
                        if (timestamp != null) {
                            try {
                                return new SimpleDateFormat("yyyyMMddHHmmssSSS").parse(timestamp).getTime();
                            } catch (ParseException e) {

                            }
                        }
                        return recordTimestamp;
                    }
                });
        SingleOutputStreamOperator<ControlMsg> controlStream = source.assignTimestampsAndWatermarks(strategy).keyBy(new KeySelector<Map, String>() {
            @Override
            public String getKey(Map value) throws Exception {
                Map<String, Object> map = value;
                if (map.containsKey("operateType")) {
                    return (String) map.get("accountNo");
                }
                return (String) map.get("oppositeAcctNo");
            }
        }).process(new LoanFlowControlFunction());
        controlStream.print();
        controlStream.process(new SinkMapFunction("IDATA-EPLUS_RCPM_EVENT", "black_list_ctrl"))
                .addSink(new RocketMQSink<>(getProducerProps()));
//        source.process(new SourceMapFunction())
//                .process(new SinkMapFunction("FLINK_SINK", "*"))
//                .addSink(
//                        new RocketMQSink(producerProps)
//                                .withBatchFlushOnCheckpoint(true)
//                                .withBatchSize(32)
//                                .withAsync(true))
//                .setParallelism(2);
        env.execute("loan-flow-control");
    }
}
