package com.zz.flink.table.source;

import org.apache.flink.table.connector.source.AsyncTableFunctionProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;

public class HttpLookupTableSource implements LookupTableSource {

    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
        return AsyncTableFunctionProvider.of(new AsyncHttpTableFunction());
    }

    @Override
    public DynamicTableSource copy() {
        return new HttpLookupTableSource();
    }

    @Override
    public String asSummaryString() {
        return "http lookup";
    }
}
