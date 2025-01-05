package com.example.pxf.parquet;

import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.io.api.GroupConverter;
import io.delta.standalone.data.RowRecord;
import io.delta.standalone.types.StructType;

public class RowMaterializer extends RecordMaterializer<RowRecord> {
    private final RowConverter rowConverter;

    public RowMaterializer(StructType schema) {
        this.rowConverter = new RowConverter(schema);
    }

    @Override
    public RowRecord getCurrentRecord() {
        return rowConverter.getCurrentRowRecord();
    }

    @Override
    public GroupConverter getRootConverter() {
        return rowConverter;
    }
}

