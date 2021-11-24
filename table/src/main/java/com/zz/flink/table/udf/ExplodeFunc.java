package com.zz.flink.table.udf;

import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.types.CollectionDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.inference.TypeStrategy;
import org.apache.flink.types.Row;

import java.util.List;
import java.util.Optional;

public class ExplodeFunc extends TableFunction<Row> {

    public void eval(Row[] list) {
        for (Row row : list) {
            this.collect(row);
        }
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
        return TypeInference.newBuilder()
                .outputTypeStrategy(new TypeStrategy() {
                    @Override
                    public Optional<DataType> inferType(CallContext callContext) {
                        List<DataType> argumentDataTypes = callContext.getArgumentDataTypes();
                        CollectionDataType arrayType = (CollectionDataType) argumentDataTypes.get(0);
                        return Optional.of(arrayType.getElementDataType());
                    }
                }).build();
    }
}
