package com.example.pxf;

import java.io.File;
import java.util.logging.Logger;
import java.util.List;
import io.delta.kernel.types.*;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;

public class DeltaUtilities {
    private static final Logger LOG = Logger.getLogger(DeltaTableFragmenter.class.getName());

    public static boolean isDeltaTable(String tablePath) {
        // check the tablePath/_delta_log directory exists
        return new File(tablePath + "/_delta_log").exists();
    }

    public static boolean isParentPath(String path) {
        // check if the path is a parent path of a delta table
        if (isDeltaTable(path)) {
            return false;
        }
        // check if the path contains a delta table 
        File[] files = new File(path).listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isDirectory() && isDeltaTable(file.getPath())) {
                    continue;
                } else {
                    // should error out here
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * Get the corresponding Parquet type for the given Greenplum column descriptor
     *
     * @param columnDescriptor contains Greenplum data type and column name
     * @return the corresponding Parquet type
     */
    private static DataType getTypeForColumnDescriptor(ColumnDescriptor columnDescriptor) {
        String typeName = columnDescriptor.columnTypeName();
        switch (typeName.toLowerCase()) {
            case "boolean":
                return BooleanType.BOOLEAN;
            case "bytea":
                return ByteType.BYTE;
            case "bigint":
            case "int8":
                return LongType.LONG;
            case "integer":
            case "int4":
                return IntegerType.INTEGER;
            case "smallint":
            case "int2":
                return ShortType.SHORT;
            case "real":
            case "float":
            case "float4":
                return FloatType.FLOAT;
            case "float8":
                return DoubleType.DOUBLE;
            case "date":
                return DateType.DATE;
            case "timestamp":
                return TimestampType.TIMESTAMP;
            case "text":
            case "varchar":
            case "bpchar":
            case "numeric":             // Greenplum numeric type is mapped to string type now
                return StringType.STRING;
            default:
                throw new UnsupportedOperationException("Unsupported data type: " + typeName);
        }
    }

    public static StructType getReadSchema(List<ColumnDescriptor> columns) {
        StructType schema = new StructType();
        for (ColumnDescriptor column : columns) {
            if (column.isProjected()) { // Skip columns that are not projected
                schema = schema.add(column.columnName(), getTypeForColumnDescriptor(column));
                LOG.info("Added column: " + column.columnName() + " with type: " + column.columnTypeName());
            }
        }
        return schema;
    }
   /**
     * Generate schema for all the supported types using column descriptors
     *
     * @param columns contains Greenplum data type and column name
     * @return the generated parquet schema used for write
     */
    public static StructType generateParquetSchema(List<ColumnDescriptor> columns) {
        StructType schema = new StructType();
        for (ColumnDescriptor column : columns) {
            schema = schema.add(column.columnName(), getTypeForColumnDescriptor(column));
            LOG.info("Added column: " + column.columnName() + " with type: " + column.columnTypeName());
        }
        return schema;
    }
}
