package com.zz.flink.cdc.stream;

import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;


public class RowDebeziumDeserializationSchema implements DebeziumDeserializationSchema<Row> {

    @Override
    public void deserialize(SourceRecord record, Collector<Row> out) throws Exception {
        Struct value = (Struct) record.value();
//        System.out.println(value);
        Schema valueSchema = record.valueSchema();
//        Struct source = (Struct) value.get("source");
//        System.out.println(source);
//        System.out.println(source.getString("db")+":"+source.getString("table"));
//        String table = source.getString("table");
        Struct after = value.getStruct(Envelope.FieldName.AFTER);
        Row row = new Row(valueSchema.fields().size());
        out.collect(row);
    }

    @Override
    public TypeInformation<Row> getProducedType() {
        return TypeInformation.of(Row.class);
    }
}
