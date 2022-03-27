package com.paic.flink.metrics;

import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.metrics.reporter.MetricReporterFactory;
import org.apache.flink.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class KafkaMetricReporterFactory implements MetricReporterFactory {

    private static final Logger LOG =
            LoggerFactory.getLogger(KafkaMetricReporterFactory.class);

    @Override
    public MetricReporter createMetricReporter(Properties properties) {
        MetricConfig config = (MetricConfig) properties;
        String servers = config.getString("servers", "localhost:9092");
        String topic = config.getString("topic", "flink_metrics");
        Map<String, String> groupingKey =
                parseGroupingKey(
                        config.getString("groupingKey", ""));
        return new KafkaMetricReporter(servers, topic, groupingKey);
    }

    static Map<String, String> parseGroupingKey(final String groupingKeyConfig) {
        if (!groupingKeyConfig.isEmpty()) {
            Map<String, String> groupingKey = new HashMap<>();
            String[] kvs = groupingKeyConfig.split(";");
            for (String kv : kvs) {
                int idx = kv.indexOf("=");
                if (idx < 0) {
                    LOG.warn("Invalid prometheusPushGateway groupingKey:{}, will be ignored", kv);
                    continue;
                }

                String labelKey = kv.substring(0, idx);
                String labelValue = kv.substring(idx + 1);
                if (StringUtils.isNullOrWhitespaceOnly(labelKey)
                        || StringUtils.isNullOrWhitespaceOnly(labelValue)) {
                    LOG.warn(
                            "Invalid groupingKey {labelKey:{}, labelValue:{}} must not be empty",
                            labelKey,
                            labelValue);
                    continue;
                }
                groupingKey.put(labelKey, labelValue);
            }

            return groupingKey;
        }

        return Collections.emptyMap();
    }
}
