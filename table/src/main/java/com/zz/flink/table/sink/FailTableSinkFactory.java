package com.zz.flink.table.sink;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.DynamicTableSinkFactory;

import java.util.Collections;
import java.util.Set;

public class FailTableSinkFactory implements DynamicTableSinkFactory {

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        return new FailTableSink(
                context.getCatalogTable().getSchema().toPhysicalRowDataType());
    }

    @Override
    public String factoryIdentifier() {
        return "fail";
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Collections.emptySet();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return Collections.emptySet();
    }
}
