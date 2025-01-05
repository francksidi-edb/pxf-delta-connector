package com.example.pxf;

import io.delta.standalone.DeltaLog;
import io.delta.standalone.Snapshot;
import io.delta.standalone.data.RowRecord;
import io.delta.standalone.types.StructField;
import io.delta.standalone.types.StructType;
import org.greenplum.pxf.api.OneField;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.model.ReadVectorizedResolver;
import org.greenplum.pxf.api.model.Resolver;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;
import java.sql.Date;
import java.math.BigDecimal;
import java.util.logging.Level;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import java.util.logging.Logger;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.type.TypeReference;
import java.util.Map;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.stream.IntStream;
import java.util.Arrays; // Add this import
import java.util.stream.Collectors; // Add this import
				    //
import org.greenplum.pxf.api.filter.FilterParser;
import io.delta.standalone.expressions.Expression;
import org.greenplum.pxf.api.filter.ColumnPredicateBuilder;
import io.delta.standalone.actions.AddFile;

import org.greenplum.pxf.api.filter.OperandNode;
import org.greenplum.pxf.api.filter.OperatorNode;
import org.greenplum.pxf.api.filter.Node;
import java.util.function.Predicate;

import io.delta.standalone.expressions.Expression;
import io.delta.standalone.expressions.EqualTo;
import io.delta.standalone.expressions.And;
import io.delta.standalone.expressions.Or;
import io.delta.standalone.expressions.Column;
import io.delta.standalone.expressions.Literal;



/**
 * Resolver for Delta Table rows into Greenplum fields with batch processing.
 */
public class DeltaTableResolverVec implements ReadVectorizedResolver, Resolver {

    private static final Logger LOG = Logger.getLogger(DeltaTableResolverVec.class.getName());
    private static final String BATCH_SIZE_PROPERTY = "batch_size";
    private static final int DEFAULT_BATCH_SIZE = 1024;

    private RequestContext context;
    private DeltaLog deltaLog;
    private Snapshot snapshot;
    private StructType schema;
    private Configuration configuration;
    private int batchSize;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private Map<String, StructField> fieldMap; // Add this field

    private Expression filterExpression; // Add this field for filtering rows


private void initializeFieldMap() {
    try {
        // Use Arrays.stream() to create a stream from the StructField array
        fieldMap = Arrays.stream(schema.getFields())
                .collect(Collectors.toMap(StructField::getName, field -> field));
    } catch (OutOfMemoryError e) {
        LOG.severe("Out of memory while initializing the field map. Consider increasing heap size or optimizing schema size.");
        throw e; // Re-throw to terminate, or handle gracefully if recovery is possible
    } catch (Exception e) {
        LOG.severe("An unexpected error occurred while initializing the field map: " + e.getMessage());
        throw new RuntimeException("Failed to initialize field map", e);
    }
}




    @Override
    public void setRequestContext(RequestContext context) {
        this.context = context;
        this.configuration = context.getConfiguration();
        initialize();
    }


    @Override
public void afterPropertiesSet() {
    LOG.info("Initializing DeltaTableResolverVec...");

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
        LOG.log(Level.SEVERE, "Error initializing DeltaTableResolverVec", e);
        throw new RuntimeException("Failed to initialize DeltaTableResolverVec", e);
    }
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



    private void initialize() {
        LOG.info("Initializing DeltaTableResolverVec...");

        String deltaTablePath = context.getDataSource();
        if (deltaTablePath == null || deltaTablePath.isEmpty()) {
            throw new IllegalArgumentException("Delta table path is not provided.");
        }

        try {
            Configuration hadoopConf = new Configuration();
            hadoopConf.set("fs.defaultFS", "file:///");
            hadoopConf.set("mapreduce.framework.name", "local");
            hadoopConf.set("hadoop.tmp.dir", System.getProperty("java.io.tmpdir") + "/hadoop");

	    String batchSizeString = context.getOption(BATCH_SIZE_PROPERTY);
            batchSize = (batchSizeString != null) ? Integer.parseInt(batchSizeString) : DEFAULT_BATCH_SIZE;
            LOG.info("Batch size initialized to: " + batchSize);

            deltaLog = DeltaLog.forTable(hadoopConf, new Path(deltaTablePath));
            snapshot = deltaLog.snapshot();
            schema = snapshot.getMetadata().getSchema();

            if (schema == null) {
                throw new IllegalStateException("Schema is null. Ensure the Delta table has valid metadata.");
            }


	    String filterString = context.getFilterString();

            if (filterString != null && !filterString.isEmpty()) {
	        LOG.info("Filter Passed: " + filterString);
		FilterParser filterParser = new FilterParser();
                Node filterNode = filterParser.parse(filterString);

            // Build Delta Expression from the parsed filterNode
               PredicateBuilder predicateBuilder = new PredicateBuilder(schema);
               filterExpression = predicateBuilder.buildExpression((OperatorNode) filterNode);
	       LOG.info("Expression Built: " + filterExpression.toString());
	    }
	    else 
	       LOG.info("Expression is empty");

            LOG.info("Schema successfully loaded: " + schema.getTreeString());
	    initializeFieldMap();
            LOG.info("Initializing FieldMap with batchsize of" + batchSize);
        } catch (Exception e) {
            LOG.severe("Error initializing DeltaTableResolverVec: " + e.getMessage());
            throw new RuntimeException("Failed to initialize DeltaTableResolverVec", e);
        }
    }



@Override
public List<List<OneField>> getFieldsForBatch(OneRow batch) {
    if (schema == null) {
        throw new IllegalStateException("Schema is not initialized. Check Delta table metadata.");
    }

    Object rowData = batch.getData();
    if (!(rowData instanceof List)) {
        throw new IllegalArgumentException("Batch data type is not supported: " +
                (rowData != null ? rowData.getClass().getName() : "null"));
    }

    @SuppressWarnings("unchecked")
    List<RowRecord> records = (List<RowRecord>) rowData;

    //LOG.info(String.format("Processing batch of %d rows", records.size()));

    final Instant start = Instant.now();

    List<List<OneField>> resolvedBatch = new ArrayList<>(records.size());
    int filteredCount = 0;

    for (RowRecord record : records) {
        // Evaluate the expression for the current record
        if (filterExpression == null || evaluateExpression(filterExpression, record)) {
            resolvedBatch.add(processRecord(record));
        } else {
		filteredCount++;
        }
    }
/*
    final long elapsedNanos = Duration.between(start, Instant.now()).toMillis();
    LOG.info(String.format("Processed batch of %d rows in %d milliseconds", resolvedBatch.size(), elapsedNanos));
     LOG.info(String.format("Filtered out %d rows that did not match the criteria", filteredCount));

*/

    return resolvedBatch;
}


private boolean evaluateExpression(Expression expression, RowRecord record) {
    // Use the eval method to evaluate the expression directly on the RowRecord
    Object result = expression.eval(record);

    // Ensure the result is a boolean for logical expressions like EqualTo
    if (result instanceof Boolean) {
        return (Boolean) result;
    }

    throw new UnsupportedOperationException("Expression evaluation resulted in non-boolean value: " + result);
}



private List<OneField> processRecord(RowRecord record) {
    // Cache tuple description for efficiency
    List<ColumnDescriptor> tupleDescription = context.getTupleDescription();
    List<OneField> fields = new ArrayList<>(tupleDescription.size());

    for (ColumnDescriptor column : tupleDescription) {
        String columnName = column.columnName();
        Object value = extractValue(record, columnName,schema); // Simplify by removing schema dependency

        fields.add(new OneField(column.getDataType().getOID(), value)); // Combine creation and addition
    }

    return fields;
}


private Object extractValue(RowRecord record, String columnName, StructType schema) {
    if (schema == null) {
        throw new IllegalStateException("Schema is null. Ensure schema is initialized before extracting values.");
    }

    if (fieldMap == null) {
        // Initialize fieldMap lazily in case it wasn't done earlier
        initializeFieldMap();
    }

    boolean dateAsString = "true".equalsIgnoreCase(context.getOption("date_as_string"));

    // Use cached fieldMap to retrieve the field
    StructField field = fieldMap.get(columnName);
    if (field == null) {
        throw new IllegalArgumentException("Column " + columnName + " does not exist in the schema.");
    }

    if (record.isNullAt(columnName)) {
        return null;
    }

    // Extract the value based on the field's data type
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
            Object dateValue = record.getDate(columnName);
            return dateAsString ? dateValue.toString() : dateValue;
        case "timestamp":
            return record.getTimestamp(columnName).toString();
        case "decimal":
            return record.getBigDecimal(columnName); // Avoid unnecessary BigDecimal creation
        default:
            throw new UnsupportedOperationException("Unsupported column type: " +
                    field.getDataType().getTypeName());
    }
}




    @Override
    public List<OneField> getFields(OneRow row) {
        throw new UnsupportedOperationException("Unsupported Operation GetField");
    }


    @Override
    public OneRow setFields(List<OneField> record) {
        throw new UnsupportedOperationException("Writing is not supported by DeltaTableResolverVec.");
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

}

