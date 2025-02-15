package com.example.pxf;

import java.io.File;
import java.util.logging.Logger;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.delta.kernel.types.*;
import io.delta.kernel.expressions.*;

import org.greenplum.pxf.api.filter.FilterParser;
import org.greenplum.pxf.api.filter.InOperatorTransformer;
import org.greenplum.pxf.api.filter.Node;
import org.greenplum.pxf.api.filter.TreeVisitor;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.greenplum.pxf.plugins.hdfs.filter.BPCharOperatorTransformer;
import org.greenplum.pxf.plugins.hdfs.parquet.ParquetOperatorPruner;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type.Repetition;
import com.example.pxf.parquet.ParquetRecordFilterBuilder;
import org.greenplum.pxf.api.filter.TreeTraverser;

public class DeltaUtilities {
    private static final Logger LOG = Logger.getLogger(DeltaTableFragmenter.class.getName());
    private static final TreeTraverser TRAVERSER = new TreeTraverser();
    private static final TreeVisitor IN_OPERATOR_TRANSFORMER = new InOperatorTransformer();

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

    private static Type dataTypeToParquetType(DataType dataType) {
        switch (dataType.toString().toUpperCase()) {
            case "BOOLEAN":
                return Types.primitive(PrimitiveTypeName.BOOLEAN, Repetition.OPTIONAL).named("boolean");
            case "BYTE":
                return Types.primitive(PrimitiveTypeName.INT32, Repetition.OPTIONAL).named("byte");
            case "LONG":
                return Types.primitive(PrimitiveTypeName.INT64, Repetition.OPTIONAL).named("long");
            case "INTEGER":
                return Types.primitive(PrimitiveTypeName.INT32, Repetition.OPTIONAL).named("integer");
            case "SHORT":
                return Types.primitive(PrimitiveTypeName.INT32, Repetition.OPTIONAL).named("short");
            case "FLOAT":
                return Types.primitive(PrimitiveTypeName.FLOAT, Repetition.OPTIONAL).named("float");
            case "DOUBLE":
                return Types.primitive(PrimitiveTypeName.DOUBLE, Repetition.OPTIONAL).named("double");
            case "DATE":
                return Types.primitive(PrimitiveTypeName.INT32, Repetition.OPTIONAL).named("date");
            case "TIMESTAMP":
                return Types.primitive(PrimitiveTypeName.INT64, Repetition.OPTIONAL).named("timestamp");
            case "STRING":
                return Types.primitive(PrimitiveTypeName.BINARY, Repetition.OPTIONAL).named("string"); 
            default:
                throw new UnsupportedOperationException("Unsupported data type: " + dataType.toString());
        }
    }

    /**
     * Builds a map of names to Types from the read schema, the map allows
     * easy access from a given column name to the schema {@link Type}.
     *
     * @param readSchema the read schema for the delta file
     * @return a map of field names to types
     */
    private static Map<String, Type> getOriginalFieldsMap(StructType readSchema) {
        Map<String, Type> readFields = new HashMap<>(readSchema.fields().size() * 2);

        // We need to add the original name and lower cased name to
        // the map to support mixed case where in GPDB the column name
        // was created with quotes i.e "mIxEd CaSe". When quotes are not
        // used to create a table in GPDB, the name of the column will
        // always come in lower-case
        readSchema.fields().forEach(t -> {
            String columnName = t.getName();
            readFields.put(columnName, dataTypeToParquetType(t.getDataType()));
            readFields.put(columnName.toLowerCase(), dataTypeToParquetType(t.getDataType()));
        });

        return readFields;
    }

    private static boolean isBlank(String str) {
        return str == null || str.trim().isEmpty();
    }

    /**
     * Returns the parquet record filter for the given filter string
     *
     * @param filterString      the filter string
     * @param originalFieldsMap a map of field names to types
     * @return the parquet record filter for the given filter string
     */
    public static Predicate getFilterPredicate(String filterString, StructType readSchema,  List<ColumnDescriptor> tupleDescription) {
        if (isBlank(filterString)) {
            return null;
        }
        // Get a map of the column name to Types for the given schema
        Map<String, Type> originalFieldsMap = getOriginalFieldsMap(readSchema);

        ParquetRecordFilterBuilder filterBuilder = new ParquetRecordFilterBuilder(
                tupleDescription, originalFieldsMap);
        TreeVisitor pruner = new ParquetOperatorPruner(
                tupleDescription, originalFieldsMap, ParquetRecordFilterBuilder.SUPPORTED_OPERATORS);
        TreeVisitor bpCharTransformer = new BPCharOperatorTransformer(tupleDescription);

        try {
            // Parse the filter string into a expression tree Node
            Node root = new FilterParser().parse(filterString);
            // Transform IN operators into a chain of ORs, then
            // prune the parsed tree with valid supported operators and then
            // traverse the pruned tree with the ParquetRecordFilterBuilder to
            // produce a record filter for parquet
            TRAVERSER.traverse(root, IN_OPERATOR_TRANSFORMER, pruner, bpCharTransformer, filterBuilder);
            return filterBuilder.getRecordPredicate();
        } catch (Exception e) {
            throw new RuntimeException("Error parsing filter string: " + filterString, e);
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
