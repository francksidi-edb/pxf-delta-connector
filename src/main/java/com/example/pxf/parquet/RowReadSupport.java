package com.example.pxf.parquet;

import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.hadoop.conf.Configuration;
import io.delta.standalone.types.StructType;
import io.delta.standalone.data.RowRecord;
import org.apache.parquet.schema.MessageType;


import java.util.Map;

public class RowReadSupport extends ReadSupport<RowRecord> {
    private final StructType schema;

    public RowReadSupport(StructType schema) {
        this.schema = schema;
    }

    @Override
    public ReadContext init(Configuration configuration, Map<String, String> keyValueMetaData, MessageType fileSchema) {
        return new ReadContext(fileSchema);
    }

    @Override
    public RecordMaterializer<RowRecord> prepareForRead(Configuration configuration, Map<String, String> keyValueMetaData,
                                                        MessageType fileSchema, ReadContext readContext) {
        return new RowMaterializer(schema);
    }
}

