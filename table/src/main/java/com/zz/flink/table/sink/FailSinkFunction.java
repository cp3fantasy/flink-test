package com.zz.flink.table.sink;

import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;

public class FailSinkFunction extends RichSinkFunction<RowData> {

    private final DynamicTableSink.DataStructureConverter converter;

    public FailSinkFunction(DynamicTableSink.DataStructureConverter converter) {
        this.converter = converter;
    }

    @Override
    public void invoke(RowData value, Context context) throws Exception {
        Object data = converter.toExternal(value);
        System.out.println("fail msg: "+data);
    }
}
