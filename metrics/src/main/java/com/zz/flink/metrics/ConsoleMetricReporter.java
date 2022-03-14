package com.zz.flink.metrics;

import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.reporter.AbstractReporter;
import org.apache.flink.metrics.reporter.Scheduled;

public class ConsoleMetricReporter extends AbstractReporter implements Scheduled {

    @Override
    public void open(MetricConfig metricConfig) {

    }

    @Override
    public void close() {

    }

//    @Override
//    public void notifyOfAddedMetric(Metric metric, String metricName, MetricGroup group) {
//        System.out.println("add ---------------");
//        System.out.println(metric);
//        System.out.println(metricName);
//        System.out.println(group);
//
//        List<String> dimensionKeys = new LinkedList<>();
//        List<String> dimensionValues = new LinkedList<>();
//        for (final Map.Entry<String, String> dimension : group.getAllVariables().entrySet()) {
//            final String key = dimension.getKey();
//            dimensionKeys.add(key.substring(1, key.length() - 1));
//            dimensionValues.add(dimension.getValue());
//        }
//        System.out.println(dimensionKeys);
//        System.out.println(dimensionValues);
//        System.out.println("---------------");
//    }
//
//    @Override
//    public void notifyOfRemovedMetric(Metric metric, String metricName, MetricGroup metricGroup) {
//        System.out.println("remove---------------");
//        System.out.println(metric);
//        System.out.println(metricName);
//        System.out.println(metricGroup);
//        System.out.println("---------------");
//    }

    @Override
    public String filterCharacters(String input) {
        return input;
    }

    @Override
    public void report() {
        System.out.println("report");
    }
}
