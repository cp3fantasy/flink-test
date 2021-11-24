package com.zz.flink.table.udf;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.util.List;

public class ExplodeStringListFunc extends TableFunction<Row> {

    @DataTypeHint("ROW<s STRING>")
    public void eval(List<String> list){
        for(String s:list){
            this.collect(Row.of(s));
        }
    }
}
