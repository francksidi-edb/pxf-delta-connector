package com.example.pxf;

import io.delta.standalone.Snapshot;
import io.delta.standalone.actions.AddFile;
import io.delta.standalone.data.CloseableIterator;
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

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import java.util.logging.Logger;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.type.TypeReference;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.Map;
import java.util.HashMap;
import com.example.pxf.CustomRowRecord;
import java.util.logging.Level;
import java.sql.Date;
import java.math.BigDecimal;



/**
 * Resolver for Delta Table rows into Greenplum fields.
 */
public class DeltaTableResolver implements Resolver {

	private static final Logger LOG = Logger.getLogger(DeltaTableResolver.class.getName());

	private RequestContext context;
	private DeltaLog deltaLog;
private Snapshot snapshot;
private StructType schema;
private List<StructField> fields;
private List<ColumnDescriptor> columns;
private Configuration configuration;
private int batchSize;
private static final int DEFAULT_BATCH_SIZE = 1000;
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

/*
    @Override
    public List<OneField> getFields(OneRow row) {
        RowRecord record = (RowRecord) row.getData();
        StructType schema = (StructType) context.getMetadata();

        List<OneField> fields = new ArrayList<>();

        for (ColumnDescriptor column : context.getTupleDescription()) {
            String columnName = column.columnName();
            DataType columnType = column.getDataType();
            Object value = extractValue(record, columnName, schema);

            OneField oneField = new OneField();
            oneField.type = columnType.getOID();
            oneField.val = value;
            fields.add(oneField);

        }

        return fields;
    }
*/
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
    LOG.info("Processed Row: " + rowDetails);
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



/*

    private Object extractValue(RowRecord row, String columnName, StructType schema) {
        StructField field = schema.get(columnName);

        if (row.isNullAt(columnName)) {
            return null;
        }

        switch (field.getDataType().getTypeName().toLowerCase()) {
            case "integer":
                return row.getInt(columnName);
            case "double":
                return row.getDouble(columnName);
            case "string":
                return row.getString(columnName);
            case "boolean":
                return row.getBoolean(columnName);
            case "long":
                return row.getLong(columnName);
            case "date":
                return row.getDate(columnName).toString();
            case "timestamp":
                return row.getTimestamp(columnName).toString();
            default:
                throw new UnsupportedOperationException("Unsupported column type: " + field.getDataType().getTypeName());
        }
    }


    */


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




/*
    @Override
public void afterPropertiesSet() {
    LOG.info("Initializing DeltaTableResolver properties...");

    // Validate and initialize the Delta table path
    String deltaTablePath = context.getDataSource();
    if (deltaTablePath == null || deltaTablePath.trim().isEmpty()) {
        throw new IllegalArgumentException("Delta table path must be provided in the data source.");
    }
    LOG.info("Initializing Delta for table path: " + deltaTablePath);

    // Initialize Hadoop configuration for DeltaLog
    Configuration hadoopConf = initializeHadoopConfiguration();

    // Load the Delta table's metadata
    try {
        deltaLog = DeltaLog.forTable(hadoopConf, new Path(deltaTablePath));
        snapshot = deltaLog.snapshot();
        schema = snapshot.getMetadata().getSchema();
        fields = List.of(schema.getFields());
        LOG.info("Loaded Delta table schema: " +  schema.getTreeString());
    } catch (Exception e) {
        throw new RuntimeException("Failed to initialize Delta table metadata.", e);
    }

    // Validate and log tuple descriptions
    columns = context.getTupleDescription();
    if (columns.isEmpty()) {
        throw new IllegalArgumentException("No columns found in the request context.");
    }
    LOG.info("Tuple description: " + columns);

    // Optional: Load custom configurations
    String batchSizeString = configuration.get("delta.batch.size");
    if (batchSizeString != null && !batchSizeString.trim().isEmpty()) {
    try {
        batchSize = Integer.parseInt(batchSizeString);
        LOG.info("Batch size for Delta table reads: " + batchSize);
    } catch (NumberFormatException e) {
        throw new IllegalArgumentException("Invalid batch size specified: " + batchSizeString, e);
    }
} else {
    batchSize = DEFAULT_BATCH_SIZE;
    LOG.info("Using default batch size: " + batchSize);
}


    LOG.info("DeltaTableResolver properties successfully initialized.");
}
*/

@Override
public void afterPropertiesSet() {
    LOG.info("Initializing DeltaTableResolver...");

    String deltaTablePath = context.getDataSource();
    if (deltaTablePath == null || deltaTablePath.isEmpty()) {
        throw new IllegalArgumentException("Delta table path is not provided.");
    }
    LOG.info("Delta table path: " + deltaTablePath);

    try {
        // Initialize DeltaLog and schema
        Configuration hadoopConf = initializeHadoopConfiguration();
        deltaLog = DeltaLog.forTable(hadoopConf, new Path(deltaTablePath));
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
    public OneRow setFields(List<OneField> record) {
        throw new UnsupportedOperationException("Writing is not supported by DeltaTableResolver.");
    }
}


