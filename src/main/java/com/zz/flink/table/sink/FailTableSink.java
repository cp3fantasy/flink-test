package com.zz.flink.table.sink;

import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.types.DataType;

public class FailTableSink implements DynamicTableSink {

    private final DataType type;

    public FailTableSink(DataType type) {
        this.type = type;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return ChangelogMode.insertOnly();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        DataStructureConverter converter = context.createDataStructureConverter(type);
        return SinkFunctionProvider.of(new FailSinkFunction(converter));
    }

    @Override
    public DynamicTableSink copy() {
        return new FailTableSink(this.type);
    }

    @Override
    public String asSummaryString() {
        return "handle fail msg";
    }
}
