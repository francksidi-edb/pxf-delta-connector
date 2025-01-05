package com.example.pxf.parquet;

import org.apache.parquet.io.api.PrimitiveConverter;
import java.util.Map;
import org.apache.parquet.io.api.Binary;


public class FieldConverter extends PrimitiveConverter {
    private final String fieldName;
    private final Map<String, Object> currentRow;

    public FieldConverter(String fieldName, Map<String, Object> currentRow) {
        this.fieldName = fieldName;
        this.currentRow = currentRow;
    }

    @Override
    public void addBinary(Binary value) {
        currentRow.put(fieldName, value.toStringUsingUTF8());
    }

    @Override
    public void addBoolean(boolean value) {
        currentRow.put(fieldName, value);
    }

    @Override
    public void addDouble(double value) {
        currentRow.put(fieldName, value);
    }

    @Override
    public void addFloat(float value) {
        currentRow.put(fieldName, value);
    }

    @Override
    public void addInt(int value) {
        currentRow.put(fieldName, value);
    }

    @Override
    public void addLong(long value) {
        currentRow.put(fieldName, value);
    }
}


