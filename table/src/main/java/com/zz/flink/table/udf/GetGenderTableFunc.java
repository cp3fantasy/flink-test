package com.zz.flink.table.udf;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.util.Random;

public class GetGenderTableFunc extends TableFunction<Row> {

    @FunctionHint(output = @DataTypeHint("ROW<gender STRING, status STRING>"))
    public void eval(String key) {
        System.out.println("eval "+ key);
        if (new Random().nextInt(3) > 0) {
            String gender = key.hashCode() % 2 == 0 ? "M" : "F";
            this.collect(Row.of(gender, "success"));
        } else {
            this.collect(Row.of(null, "fail"));
        }
    }
}
