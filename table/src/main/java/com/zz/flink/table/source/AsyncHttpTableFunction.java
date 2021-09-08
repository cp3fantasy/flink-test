package com.zz.flink.table.source;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import java.util.*;
import java.util.concurrent.CompletableFuture;

public class AsyncHttpTableFunction extends AsyncTableFunction<RowData> {

    private String url;

    public void eval(CompletableFuture<Collection<RowData>> result, String key) {
        GenericRowData row = new GenericRowData(RowKind.INSERT,2);
        if (new Random().nextInt(3) > 0) {
            String gender = key.hashCode() % 2 == 0 ? "M" : "F";
            row.setField(0,gender);
            row.setField(1,"success");
        } else {
            row.setField(1,"fail");
        }
        result.complete(Collections.singletonList(row));
    }

}
