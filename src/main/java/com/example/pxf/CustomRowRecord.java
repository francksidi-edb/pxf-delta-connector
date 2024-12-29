package com.example.pxf;

import io.delta.standalone.data.RowRecord;
import io.delta.standalone.types.StructType;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

/**
 * Custom implementation of RowRecord to support deserialization of data rows.
 */
public class CustomRowRecord implements RowRecord {

    private final StructType schema;
    private final Map<String, Object> values;

    public CustomRowRecord(StructType schema, Map<String, Object> values) {
        this.schema = schema;
        this.values = values;
    }

    @Override
    public StructType getSchema() {
        return schema;
    }

    @Override
    public int getLength() {
        return values.size();
    }

    @Override
    public boolean isNullAt(String fieldName) {
        return values.get(fieldName) == null;
    }

    @Override
    public int getInt(String fieldName) {
        Object value = values.get(fieldName);
        if (value instanceof Integer) {
            return (int) value;
        }
        if (value instanceof String) {
            return Integer.parseInt((String) value);
        }
        throw new ClassCastException("Field " + fieldName + " cannot be cast to int. Found: " + value.getClass());
    }

    @Override
    public long getLong(String fieldName) {
        Object value = values.get(fieldName);
        if (value instanceof Long) {
            return (Long) value;
        }
        if (value instanceof String) {
            return Long.parseLong((String) value);
        }
        throw new ClassCastException("Field " + fieldName + " cannot be cast to long. Found: " + value.getClass());
    }

    @Override
    public byte getByte(String fieldName) {
        Object value = values.get(fieldName);
        if (value instanceof Byte) {
            return (byte) value;
        }
        if (value instanceof String) {
            return Byte.parseByte((String) value);
        }
        throw new ClassCastException("Field " + fieldName + " cannot be cast to byte. Found: " + value.getClass());
    }

    @Override
    public short getShort(String fieldName) {
        Object value = values.get(fieldName);
        if (value instanceof Short) {
            return (short) value;
        }
        if (value instanceof String) {
            return Short.parseShort((String) value);
        }
        throw new ClassCastException("Field " + fieldName + " cannot be cast to short. Found: " + value.getClass());
    }

    @Override
    public boolean getBoolean(String fieldName) {
        Object value = values.get(fieldName);
        if (value instanceof Boolean) {
            return (boolean) value;
        }
        if (value instanceof String) {
            return Boolean.parseBoolean((String) value);
        }
        throw new ClassCastException("Field " + fieldName + " cannot be cast to boolean. Found: " + value.getClass());
    }

    @Override
    public float getFloat(String fieldName) {
        Object value = values.get(fieldName);
        if (value instanceof Float) {
            return (float) value;
        }
        if (value instanceof String) {
            return Float.parseFloat((String) value);
        }
        throw new ClassCastException("Field " + fieldName + " cannot be cast to float. Found: " + value.getClass());
    }

    @Override
    public double getDouble(String fieldName) {
        Object value = values.get(fieldName);
        if (value instanceof Double) {
            return (double) value;
        }
        if (value instanceof String) {
            return Double.parseDouble((String) value);
        }
        throw new ClassCastException("Field " + fieldName + " cannot be cast to double. Found: " + value.getClass());
    }

    @Override
    public String getString(String fieldName) {
        Object value = values.get(fieldName);
        if (value instanceof String) {
            return (String) value;
        }
        return value != null ? value.toString() : null;
    }

    @Override
    public byte[] getBinary(String fieldName) {
        return (byte[]) values.get(fieldName);
    }

    @Override
    public BigDecimal getBigDecimal(String fieldName) {
        Object value = values.get(fieldName);
        if (value instanceof BigDecimal) {
            return (BigDecimal) value;
        }
        if (value instanceof String) {
            return new BigDecimal((String) value);
        }
        throw new ClassCastException("Field " + fieldName + " cannot be cast to BigDecimal. Found: " + value.getClass());
    }

    @Override
    public Timestamp getTimestamp(String fieldName) {
        Object value = values.get(fieldName);
        if (value instanceof Timestamp) {
            return (Timestamp) value;
        }
        throw new ClassCastException("Field " + fieldName + " cannot be cast to Timestamp. Found: " + value.getClass());
    }

    @Override
    public Date getDate(String fieldName) {
        Object value = values.get(fieldName);
        if (value instanceof Date) {
            return (Date) value;
        }
        throw new ClassCastException("Field " + fieldName + " cannot be cast to Date. Found: " + value.getClass());
    }

    @Override
    public RowRecord getRecord(String fieldName) {
        return (RowRecord) values.get(fieldName);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> List<T> getList(String fieldName) {
        return (List<T>) values.get(fieldName);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <K, V> Map<K, V> getMap(String fieldName) {
        return (Map<K, V>) values.get(fieldName);
    }
}


