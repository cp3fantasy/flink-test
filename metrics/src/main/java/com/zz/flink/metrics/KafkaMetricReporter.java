package com.zz.flink.metrics;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.metrics.*;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.metrics.reporter.Scheduled;
import org.apache.flink.runtime.metrics.groups.AbstractMetricGroup;
import org.apache.flink.runtime.metrics.groups.FrontMetricGroup;
import org.apache.kafka.clients.producer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Pattern;

public class KafkaMetricReporter implements MetricReporter, Scheduled {

    protected final Logger log = LoggerFactory.getLogger(getClass());

    @VisibleForTesting
    static final char SCOPE_SEPARATOR = '_';
    private static final String POINT_DELIMITER = "\n";

    private transient Producer<String, String> producer;

    private static final SimpleDateFormat timeFormat = new SimpleDateFormat("yyyy-mm-dd hh:mm:ss");


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
        props.put("bootstrap.servers", "localhost:9092");
//        props.put("acks", "all");
        props.put("delivery.timeout.ms", 5000);
        props.put("request.timeout.ms", 2000);
        props.put("batch.size", 16384);
        props.put("linger.ms", 10);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<>(props);
    }

    @Override
    public void close() {

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
        System.out.println("report start:" + time);
//        System.out.println(gauges);
//        System.out.println(counters);
//        System.out.println(histograms);
//        System.out.println(meters);
        try {
            for (Map.Entry<Gauge<?>, MetricInfo> entry : gauges.entrySet()) {
                sendMetric(entry.getValue(), time, entry.getKey().getValue());
            }
            for (Map.Entry<Counter, MetricInfo> entry : counters.entrySet()) {
                sendMetric(entry.getValue(), time, entry.getKey().getCount());
            }
            for (Map.Entry<Meter, MetricInfo> entry : meters.entrySet()) {
                sendMetric(entry.getValue(), time, entry.getKey().getRate());
            }
            for (Map.Entry<Histogram, MetricInfo> entry : histograms.entrySet()) {
                sendMetric(entry.getValue(), time, getHistogram(entry.getKey()));
            }
        } catch (ConcurrentModificationException e) {
            report();
        }
        System.out.println("report end");
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
        ProducerRecord<String, String> record = new ProducerRecord<>("flink_metrics", metricInfoString);
        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (exception != null) {
                    exception.printStackTrace();
                }
            }
        });
    }

    private static String getScopedName(String metricName, MetricGroup group) {
        return ((FrontMetricGroup<AbstractMetricGroup<?>>) group)
                .getLogicalScope(CHARACTER_FILTER, SCOPE_SEPARATOR);
//        return group.getMetricIdentifier(metricName, CHARACTER_FILTER);
    }

    private static Map<String, String> getTags(MetricGroup group) {
        // Keys are surrounded by brackets: remove them, transforming "<name>" to "name".
        Map<String, String> tags = new HashMap<>();
        for (Map.Entry<String, String> variable : group.getAllVariables().entrySet()) {
            String name = variable.getKey();
            tags.put(name.substring(1, name.length() - 1), variable.getValue());
        }
        return tags;
    }


}
