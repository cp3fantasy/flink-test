package com.zz.flink.metrics;

import org.apache.flink.metrics.*;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.metrics.reporter.Scheduled;
import org.apache.flink.runtime.metrics.groups.AbstractMetricGroup;
import org.apache.flink.runtime.metrics.groups.FrontMetricGroup;
import org.apache.flink.util.StringUtils;
import org.apache.kafka.clients.producer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Pattern;

public class KafkaMetricReporter implements MetricReporter, Scheduled {

    protected final Logger log = LoggerFactory.getLogger(getClass());

    static final char SCOPE_SEPARATOR = '_';

    private transient String topic;

    private transient Producer<String, String> producer;

    private static final SimpleDateFormat timeFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

    private static final CharacterFilter CHARACTER_FILTER =
            new CharacterFilter() {
                private final Pattern notAllowedCharacters = Pattern.compile("[^a-zA-Z0-9:_]");

                @Override
                public String filterCharacters(String input) {
                    return notAllowedCharacters.matcher(input).replaceAll("_");
                }
            };

    protected final Map<Gauge<?>, MetricInfo> gauges = new HashMap<>();
    protected final Map<Counter, MetricInfo> counters = new HashMap<>();
    protected final Map<Histogram, MetricInfo> histograms = new HashMap<>();
    protected final Map<Meter, MetricInfo> meters = new HashMap<>();

    @Override
    public void open(MetricConfig config) {
        Properties props = new Properties();
        String servers = config.getString("servers", "localhost:9092");
        topic = config.getString("topic", "flink_metrics");
        props.put("bootstrap.servers", servers);
//        props.put("acks", "all");
        props.put("delivery.timeout.ms", 5000);
        props.put("request.timeout.ms", 2000);
        props.put("batch.size", 16384);
        props.put("linger.ms", 10);
        props.put("buffer.memory", 1024 * 1024);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<>(props);
    }

    @Override
    public void close() {
        producer.close();
    }

    @Override
    public void notifyOfAddedMetric(Metric metric, String metricName, MetricGroup group) {
        final MetricInfo info = new MetricInfo();
        info.setName(getScopedName(metricName, group));
        info.setTags(getTags(group));
        synchronized (this) {
            if (metric instanceof Counter) {
                counters.put((Counter) metric, info);
            } else if (metric instanceof Gauge) {
                gauges.put((Gauge<?>) metric, info);
            } else if (metric instanceof Histogram) {
                histograms.put((Histogram) metric, info);
            } else if (metric instanceof Meter) {
                meters.put((Meter) metric, info);
            } else {
                log.warn(
                        "Cannot add unknown metric type {}. This indicates that the reporter "
                                + "does not support this metric type.",
                        metric.getClass().getName());
            }
        }
    }

    @Override
    public void notifyOfRemovedMetric(Metric metric, String metricName, MetricGroup group) {
        synchronized (this) {
            if (metric instanceof Counter) {
                counters.remove(metric);
            } else if (metric instanceof Gauge) {
                gauges.remove(metric);
            } else if (metric instanceof Histogram) {
                histograms.remove(metric);
            } else if (metric instanceof Meter) {
                meters.remove(metric);
            } else {
                log.warn(
                        "Cannot remove unknown metric type {}. This indicates that the reporter "
                                + "does not support this metric type.",
                        metric.getClass().getName());
            }
        }
    }


    @Override
    public void report() {
        String time = timeFormat.format(new Date());
        log.info("report start at {}", time);
        try {
            log.info("report gauges:{}", gauges.size());
            for (Map.Entry<Gauge<?>, MetricInfo> entry : gauges.entrySet()) {
                sendMetric(entry.getValue(), time, entry.getKey().getValue());
            }
            log.info("report counters:{}", counters.size());
            for (Map.Entry<Counter, MetricInfo> entry : counters.entrySet()) {
                sendMetric(entry.getValue(), time, entry.getKey().getCount());
            }
            log.info("report meters:{}", meters.size());
            for (Map.Entry<Meter, MetricInfo> entry : meters.entrySet()) {
                sendMetric(entry.getValue(), time, entry.getKey().getRate());
            }
            log.info("report histograms:{}", histograms.size());
            for (Map.Entry<Histogram, MetricInfo> entry : histograms.entrySet()) {
                sendMetric(entry.getValue(), time, getHistogram(entry.getKey()));
            }
        } catch (ConcurrentModificationException e) {
            log.warn("report error ", e);
        }
        log.info("report end at {}", timeFormat.format(new Date()));
    }

    private Object getHistogram(Histogram histogram) {
        HistogramStatistics stat = histogram.getStatistics();
        Map<String, Object> map = new HashMap<>();
        map.put("count", histogram.getCount());
        map.put("min", stat.getMin());
        map.put("max", stat.getMax());
        map.put("mean", stat.getMean());
        map.put("stddev", stat.getStdDev());
        map.put("p50", stat.getQuantile(.50));
        map.put("p95", stat.getQuantile(95));
        map.put("p99", stat.getQuantile(.99));
        map.put("p999", stat.getQuantile(.999));
        return map;
    }

    private void sendMetric(MetricInfo metricInfo, String time, Object value) {
        metricInfo.setTime(time);
        metricInfo.setValue(value);
        String metricInfoString = metricInfo.toString();
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, metricInfoString);
        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (exception != null) {
                    log.error("kafka send error", exception);
                }
            }
        });
    }

    private static String getScopedName(String metricName, MetricGroup group) {
        return ((FrontMetricGroup<AbstractMetricGroup<?>>) group)
                .getLogicalScope(CHARACTER_FILTER, SCOPE_SEPARATOR) + SCOPE_SEPARATOR + metricName;
    }

    private static Map<String, String> getTags(MetricGroup group) {
        // Keys are surrounded by brackets: remove them, transforming "<name>" to "name".
        Map<String, String> tags = new HashMap<>();
        for (Map.Entry<String, String> variable : group.getAllVariables().entrySet()) {
            String name = variable.getKey();
            String tagValue = variable.getValue();
            if (!StringUtils.isNullOrWhitespaceOnly(tagValue)) {
                tags.put(name.substring(1, name.length() - 1), variable.getValue());
            }
        }
        return tags;
    }


}
