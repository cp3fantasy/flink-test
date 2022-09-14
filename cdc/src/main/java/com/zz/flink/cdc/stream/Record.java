package com.zz.flink.cdc.stream;

import org.apache.flink.types.RowKind;

import java.util.Map;

public class Record {

    private String table;

    private Map<String,Object> data;

    private RowKind rowKind;

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public Map<String, Object> getData() {
        return data;
    }

    public void setData(Map<String, Object> data) {
        this.data = data;
    }

    public RowKind getRowKind() {
        return rowKind;
    }

    public void setRowKind(RowKind rowKind) {
        this.rowKind = rowKind;
    }

    @Override
    public String toString() {
        return "Record{" +
                "table='" + table + '\'' +
                ", data=" + data +
                ", rowKind=" + rowKind +
                '}';
    }
}
