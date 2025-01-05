package com.example.pxf.parquet;

import io.delta.standalone.data.RowRecord;
import io.delta.standalone.types.StructField;
import io.delta.standalone.types.StructType;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;

import java.util.*;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;

/**
 * Converts Parquet group nodes into a RowRecord representation for use with Delta.
 */
public class RowConverter extends GroupConverter {

    private final StructType schema;
    private final Map<String, Object> currentRow;
    private final FieldConverter[] fieldConverters;

    public RowConverter(StructType schema) {
        this.schema = schema;
        this.currentRow = new HashMap<>();
        this.fieldConverters = new FieldConverter[schema.getFields().length];

        // Initialize field converters based on schema fields
        for (int i = 0; i < schema.getFields().length; i++) {
            String fieldName = schema.getFields()[i].getName();
            fieldConverters[i] = new FieldConverter(fieldName, currentRow);
        }
    }

    /**
     * Returns the converter for the specified field index.
     *
     * @param fieldIndex the field index in the group
     * @return the corresponding converter
     */
    @Override
    public Converter getConverter(int fieldIndex) {
        return fieldConverters[fieldIndex];
    }

    /**
     * Called at the beginning of processing a group.
     */
    @Override
    public void start() {
        currentRow.clear();
    }

    /**
     * Called at the end of processing a group.
     */
    @Override
    public void end() {
        // Processing for the group ends here
    }

    /**
     * Returns the current row as a RowRecord.
     *
     * @return the current RowRecord
     */
    public RowRecord getCurrentRowRecord() {
        return new RowRecord() {
            @Override
            public <K, V> Map<K, V> getMap(String key) {
                Object value = currentRow.get(key);
                if (value instanceof Map) {
                    return (Map<K, V>) value;
                }
                return Collections.emptyMap();
            }

	    @Override
            public <T> List<T> getList(String key) {
                Object value = currentRow.get(key);
                if (value instanceof List) {
                    return (List<T>) value;
                }
                return Collections.emptyList();
            }
	    @Override
            public RowRecord getRecord(String key) {
                Object value = currentRow.get(key);
                if (value instanceof RowRecord) {
                    return (RowRecord) value;
                }
                throw new IllegalArgumentException("Field '" + key + "' is not a nested record.");
            }
	      @Override
            public BigDecimal getBigDecimal(String fieldName) {
                return (BigDecimal) currentRow.get(fieldName);
            }

            @Override
            public byte[] getBinary(String fieldName) {
                return (byte[]) currentRow.get(fieldName);
            }

            @Override
            public boolean getBoolean(String fieldName) {
                return (boolean) currentRow.get(fieldName);
            }

            @Override
            public byte getByte(String fieldName) {
                return (byte) currentRow.get(fieldName);
            }

            @Override
            public Date getDate(String fieldName) {
                return (Date) currentRow.get(fieldName);
            }

            @Override
            public double getDouble(String fieldName) {
                return (double) currentRow.get(fieldName);
            }

            @Override
            public float getFloat(String fieldName) {
                return (float) currentRow.get(fieldName);
            }

            @Override
            public int getInt(String fieldName) {
                return (int) currentRow.get(fieldName);
            }

            @Override
            public int getLength() {
                return schema.getFields().length;
            }
	    @Override
            public StructType getSchema() {
                return schema;
            }

            @Override
            public short getShort(String fieldName) {
                return (short) currentRow.get(fieldName);
            }

            @Override
            public String getString(String fieldName) {
                return (String) currentRow.get(fieldName);
            }

            @Override
            public Timestamp getTimestamp(String fieldName) {
                return (Timestamp) currentRow.get(fieldName);
            }

            @Override
            public boolean isNullAt(String fieldName) {
                return !currentRow.containsKey(fieldName) || currentRow.get(fieldName) == null;
            }
	    @Override
            public long getLong(String fieldName) {
                return (long) currentRow.get(fieldName);
            }

        };
    }
}


