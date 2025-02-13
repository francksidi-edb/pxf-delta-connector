package com.example.pxf;

import io.delta.standalone.Snapshot;
import io.delta.standalone.data.RowRecord;
import io.delta.standalone.types.StructField;
import io.delta.standalone.types.StructType;
import org.greenplum.pxf.api.OneField;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.model.Resolver;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.greenplum.pxf.api.io.DataType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import io.delta.standalone.DeltaLog;
import io.delta.kernel.data.*;
import io.delta.kernel.defaults.internal.data.vector.DefaultGenericVector;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import java.util.logging.Logger;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.type.TypeReference;

import java.util.logging.Level;
import java.sql.Date;
import java.time.Instant;
import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.TimeUnit;

import org.greenplum.pxf.api.error.UnsupportedTypeException;
import com.example.pxf.partitioning.DeltaFragmentMetadata;

/**
 * Resolver for Delta Table rows into Greenplum fields.
 */
public class DeltaTableResolver implements Resolver {

	private static final Logger LOG = Logger.getLogger(DeltaTableResolver.class.getName());

	private RequestContext context;
	private DeltaLog deltaLog;
    private StructType schema;
    private Configuration configuration;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void setRequestContext(RequestContext context) {
        this.context = context;
        this.configuration = context.getConfiguration();
    }

    private Configuration initializeHadoopConfiguration() {
        Configuration hadoopConf = new Configuration();

        // Ensure necessary values are non-null using Guava's Preconditions
        String defaultFS = Preconditions.checkNotNull("file:///", "fs.defaultFS cannot be null");
        String frameworkName = Preconditions.checkNotNull("local", "mapreduce.framework.name cannot be null");
        String tmpDir = Strings.nullToEmpty(System.getProperty("java.io.tmpdir")) + "/hadoop";

        // Set Hadoop configuration properties
        hadoopConf.set("fs.defaultFS", defaultFS);
        hadoopConf.set("mapreduce.framework.name", frameworkName);
        hadoopConf.set("hadoop.tmp.dir", tmpDir);

        LOG.info(String.format(
            "Hadoop configuration initialized with fs.defaultFS=%s, mapreduce.framework.name=%s, hadoop.tmp.dir=%s",
            defaultFS, frameworkName, tmpDir
        ));


        return hadoopConf;
    }

    private void initializeSchema() {
        String tablePath = context.getDataSource();
        if (tablePath == null || tablePath.isEmpty()) {
            throw new IllegalArgumentException("Delta table path is not provided.");
        }
        boolean isParentPath = DeltaUtilities.isParentPath(tablePath);
        if (isParentPath) {
            DeltaFragmentMetadata fragmentMeta = context.getFragmentMetadata();
            // fragmentMeta is only valid for read operations
            if (fragmentMeta != null) {
                String partitionInfo = fragmentMeta.getPartitionInfo();
                if (partitionInfo == null || partitionInfo.isEmpty()) {
                    LOG.info("Partition filter is empty for parent path: " + tablePath);
                } else {
                    tablePath = tablePath + "/" + partitionInfo;
                }
            }
        }
        LOG.info("Delta table path: " + tablePath);
        if (!DeltaUtilities.isDeltaTable(tablePath)) {
            LOG.info("Delta table is empty at path: " + tablePath);
            return;
        }
        try {
            // Initialize DeltaLog and schema
            Configuration hadoopConf = initializeHadoopConfiguration();
            deltaLog = DeltaLog.forTable(hadoopConf, new Path(tablePath));
            Snapshot snapshot = deltaLog.snapshot();

            schema = snapshot.getMetadata().getSchema();
            if (schema == null) {
                throw new IllegalStateException("Schema is null. Ensure the Delta table has valid metadata.");
            }

            LOG.info("Schema successfully loaded: " + schema.getTreeString());
        } catch (Exception e) {
            LOG.log(Level.SEVERE, "Error initializing DeltaTableResolver", e);
            throw new RuntimeException("Failed to initialize DeltaTableResolver", e);
        }
    }

@Override
public List<OneField> getFields(OneRow row) {
    if (schema == null) {
        throw new IllegalStateException("Schema is not initialized. Check Delta table metadata.");
    }

    Object rowData = row.getData();
    RowRecord record;

    // Deserialize row data
    if (rowData instanceof String) {
        record = deserializeToRowRecord((String) rowData, schema);
    } else if (rowData instanceof RowRecord) {
        record = (RowRecord) rowData;
    } else {
        throw new IllegalArgumentException("Unsupported row data type: " + rowData.getClass());
    }

    // Log row details for debugging
    logRowDetails(record, schema);

    // Extract fields based on the schema
    List<OneField> fields = new ArrayList<>();
    for (ColumnDescriptor column : context.getTupleDescription()) {
        String columnName = column.columnName();
        Object value = extractValue(record, columnName, schema);

        OneField oneField = new OneField();
        oneField.type = column.getDataType().getOID();
        oneField.val = value;

        fields.add(oneField);
    }

    return fields;
}

private void logRowDetails(RowRecord record, StructType schema) {
    StringBuilder rowDetails = new StringBuilder();
    for (StructField field : schema.getFields()) {
        Object value = extractValue(record, field.getName(), schema);
        rowDetails.append(field.getName()).append("=").append(value).append(", ");
    }
    // Log the row details
    LOG.fine("Processed Row: " + rowDetails);
}

private RowRecord deserializeToRowRecord(String rowData, StructType schema) {
    try {
        // Convert the row data into a JSON-compatible string
        String jsonCompatibleData = convertToJson(rowData);

        // Parse the JSON-compatible data into a map of field names and values
        Map<String, Object> values = objectMapper.readValue(jsonCompatibleData, new TypeReference<Map<String, Object>>() {});

        // Create and return a CustomRowRecord object with the schema and parsed values
        return new CustomRowRecord(schema, values);
    } catch (Exception e) {
        // Log the error and rethrow it as a runtime exception
        LOG.severe("Failed to deserialize data to RowRecord: " + rowData + " Error: " + e.getMessage());
        throw new RuntimeException("Failed to deserialize data to RowRecord", e);
    }
}

private String convertToJson(String rowData) {
    String[] pairs = rowData.split(", ");
    StringBuilder jsonBuilder = new StringBuilder("{");

    for (String pair : pairs) {
        String[] keyValue = pair.split("=", 2);
        if (keyValue.length == 2) {
            jsonBuilder.append("\"")
                       .append(keyValue[0].trim())
                       .append("\": ")
                       .append("\"")
                       .append(keyValue[1].trim())
                       .append("\", ");
        }
    }

    // Remove trailing comma and space, then close the JSON object
    if (jsonBuilder.length() > 1) {
        jsonBuilder.setLength(jsonBuilder.length() - 2);
    }
    jsonBuilder.append("}");

    return jsonBuilder.toString();
}

private Object extractValue(RowRecord record, String columnName, StructType schema) {
    if (schema == null) {
        throw new IllegalStateException("Schema is null. Ensure schema is initialized before extracting values.");
    }
    boolean dateAsString = "true".equalsIgnoreCase(context.getOption("date_as_string"));

    StructField field = schema.get(columnName);
    if (field == null) {
        throw new IllegalArgumentException("Column " + columnName + " does not exist in the schema.");
    }

    if (record.isNullAt(columnName)) {
        return null;
    }

    switch (field.getDataType().getTypeName().toLowerCase()) {
        case "integer":
            return record.getInt(columnName);
        case "double":
            return record.getDouble(columnName);
        case "string":
            return record.getString(columnName);
        case "boolean":
            return record.getBoolean(columnName);
        case "long":
            return record.getLong(columnName);

        case "date":
            if (dateAsString) {
                return record.getString(columnName); // Treat as String
            } else {
                return Date.valueOf(record.getString(columnName)); // Convert to Date
            }
        case "timestamp":
            return record.getTimestamp(columnName).toString();
	    case "decimal":
            return new BigDecimal(record.getString(columnName));

        default:
            throw new UnsupportedOperationException("Unsupported column type: " + field.getDataType().getTypeName());
    }
}

    @Override
    public void afterPropertiesSet() {
        LOG.info("Initializing DeltaTableResolver...");
        initializeSchema();
    }

    private ColumnVector convertToColumnVector(OneField field, int index) {
        List<ColumnDescriptor> columnDescriptors = context.getTupleDescription();
        ColumnDescriptor columnDescriptor = columnDescriptors.get(index);
        DataType columnType = columnDescriptor.getDataType();
        Object value = field.val;

        switch (columnType) {
            case SMALLINT:
                return DefaultGenericVector.fromArray(io.delta.kernel.types.ShortType.SHORT, new Short[]{(Short) value}); 
            case INTEGER:
                return DefaultGenericVector.fromArray(io.delta.kernel.types.IntegerType.INTEGER, new Integer[]{(Integer) value});
            case BIGINT:
                return DefaultGenericVector.fromArray(io.delta.kernel.types.LongType.LONG, new Long[]{(Long) value});
            case REAL:
                return DefaultGenericVector.fromArray(io.delta.kernel.types.FloatType.FLOAT, new Float[]{(Float) value});
            case FLOAT8:
                return DefaultGenericVector.fromArray(io.delta.kernel.types.DoubleType.DOUBLE, new Double[]{(Double) value});
            case TEXT:
            case VARCHAR:
                return DefaultGenericVector.fromArray(io.delta.kernel.types.StringType.STRING, new String[]{(String) value});
            case BOOLEAN:
                return DefaultGenericVector.fromArray(io.delta.kernel.types.BooleanType.BOOLEAN, new Boolean[]{(Boolean) value});
            case BYTEA:
                return DefaultGenericVector.fromArray(io.delta.kernel.types.BinaryType.BINARY, new Byte[]{(Byte) value});
            case DATE:
                java.sql.Date date = java.sql.Date.valueOf(value.toString());
                long day = TimeUnit.SECONDS.toDays(date.getTime()/1000);    //Todo: handle timezone?
                return DefaultGenericVector.fromArray(io.delta.kernel.types.DateType.DATE, new Integer[]{(Integer) (int)day});
            case TIMESTAMP:
                Instant instant = java.sql.Timestamp.valueOf(value.toString()).toInstant();
                long micros = TimeUnit.SECONDS.toMicros(instant.getEpochSecond()) + TimeUnit.NANOSECONDS.toMicros(instant.getNano());
                //LOG.info("final micros: " + micros);
                return DefaultGenericVector.fromArray(io.delta.kernel.types.TimestampType.TIMESTAMP, new Long[]{(Long) micros});
            case NUMERIC:
                return DefaultGenericVector.fromArray(io.delta.kernel.types.StringType.STRING, new String[]{(String) value});
            default:
                throw new UnsupportedTypeException(columnType.name());
        }
    }

    @Override
    public OneRow setFields(List<OneField> record) {
        ColumnVector[] vectors = new ColumnVector[record.size()];
        for (int i = 0; i < record.size(); i++) {
            OneField field = record.get(i);
            vectors[i] = convertToColumnVector(field, i);
        }
        return new OneRow(null, vectors);
    }
}


