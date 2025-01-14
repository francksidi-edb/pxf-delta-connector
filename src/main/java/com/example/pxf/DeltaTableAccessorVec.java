package com.example.pxf;

import io.delta.standalone.DeltaLog;
import io.delta.standalone.DeltaScan;
import io.delta.standalone.Snapshot;
import io.delta.standalone.data.CloseableIterator;
import io.delta.standalone.data.RowRecord;
import io.delta.standalone.types.StructField;
import io.delta.standalone.types.StructType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.greenplum.pxf.api.model.BasePlugin;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.security.SecureLogin;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.greenplum.pxf.api.utilities.SpringContext;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.model.Accessor;
import org.greenplum.pxf.api.model.ConfigurationFactory;
import org.greenplum.pxf.api.filter.FilterParser;
import io.delta.standalone.expressions.Expression;
import org.greenplum.pxf.api.filter.ColumnPredicateBuilder;
import org.greenplum.pxf.api.filter.TreeTraverser;
import io.delta.standalone.actions.AddFile;

import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import org.greenplum.pxf.api.filter.OperandNode;
import org.greenplum.pxf.api.filter.OperatorNode;
import org.greenplum.pxf.api.filter.Node;
import java.util.function.Predicate;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.type.TypeReference;
import io.delta.standalone.expressions.EqualTo;
import io.delta.standalone.expressions.And;
import io.delta.standalone.expressions.Or;




public class DeltaTableAccessorVec extends BasePlugin implements org.greenplum.pxf.api.model.Accessor {

    private static final Logger LOG = Logger.getLogger(DeltaTableAccessor.class.getName());
    private DeltaLog deltaLog;
    private Snapshot snapshot;
    private StructType schema;
    private List<StructField> fields;
    private Iterator<String> fileIterator;
    private CloseableIterator<RowRecord> rowIterator;
    private int batchSize = 1024;
    private static final int DEFAULT_BATCH_SIZE = 1024; // Default batch size if none is provided
    private String filterColumn;
private String filterOperator;
private Object filterValue;


    @Override
    public void afterPropertiesSet() {
        // No additional initialization required
    }


    private String mapColumnIndexToName(int columnIndex) {
    List<ColumnDescriptor> tupleDescription = context.getTupleDescription();
    if (columnIndex >= 0 && columnIndex < tupleDescription.size()) {
        return tupleDescription.get(columnIndex).columnName();
    }
    throw new IllegalArgumentException("Invalid column index: " + columnIndex);
}

@Override
public boolean openForRead() {
    try {
        String deltaTablePath = context.getDataSource();
        LOG.info("Opening DeltaLog for table path: " + deltaTablePath);

        Configuration hadoopConf = initializeHadoopConfiguration();
        deltaLog = DeltaLog.forTable(hadoopConf, new Path(deltaTablePath));
        snapshot = deltaLog.snapshot();
        schema = snapshot.getMetadata().getSchema();
        fields = List.of(schema.getFields());

        LOG.info("Snapshot version: " + snapshot.getVersion());
        LOG.info("Schema fields: " + fields);

        // Log partition columns
        List<String> partitionColumns = snapshot.getMetadata().getPartitionColumns();
        LOG.info("Partition Columns: " + partitionColumns);

        // Determine batch size
        String batchSizeParam = context.getOption("batch_size");
        batchSize = (batchSizeParam != null) ? Integer.parseInt(batchSizeParam) : DEFAULT_BATCH_SIZE;
        LOG.info("Batch size set to: " + batchSize);


	/*

        // Handle predicate pushdown
        final Expression deltaExpression = (context.getFilterString() != null && !context.getFilterString().isEmpty())
                ? buildDeltaExpression(context.getFilterString())
                : null;

        if (deltaExpression != null) {
            LOG.info("Expression Built: " + deltaExpression.toString());
        } else {
            LOG.info("No filter condition provided.");
        }
	*/

        // Collect and prune files based on filter expression
        List<AddFile> files = snapshot.getAllFiles();
        LOG.info("Total files in snapshot: " + files.size());

	/*
        List<AddFile> prunedFiles = files.stream()
                .filter(file -> shouldIncludeFile(file, deltaExpression)) // Use deltaExpression
                .collect(Collectors.toList());

        LOG.info("Files after pruning: " + prunedFiles.size());
	

        // Log details of pruned files
        for (AddFile addFile : prunedFiles) {
            Map<String, String> partitionValues = addFile.getPartitionValues();
            LOG.info("File: " + addFile.getPath() + ", Partition Values: " + partitionValues);
        }

        fileIterator = prunedFiles.stream()
                .map(AddFile::getPath)
                .iterator();

        LOG.info("Delta table opened successfully for reading.");
        return openNextFile();
*/
	 fileIterator = snapshot.getAllFiles().stream()
                    .map(file -> file.getPath())
                    .collect(Collectors.toList())
                    .iterator();

            LOG.info("Delta table opened successfully for reading.");
            return openNextFile(); // Open the first file for reading rows


    } catch (Exception e) {
        LOG.log(Level.SEVERE, "Error opening Delta table for reading", e);
        return false;
    }
}

private Expression buildDeltaExpression(String filterString) {
    try {
        FilterParser filterParser = new FilterParser();
        Node filterNode = filterParser.parse(filterString);

        PredicateBuilder predicateBuilder = new PredicateBuilder(schema);
        return predicateBuilder.buildExpression((OperatorNode) filterNode);
    } catch (Exception e) {
        LOG.log(Level.SEVERE, "Error building Delta Expression from filter", e);
        throw new RuntimeException("Failed to build Delta Expression", e);
    }
}


private String normalizeColumnName(String columnName) {
    if (columnName.startsWith("Column(") && columnName.endsWith(")")) {
        return columnName.substring(7, columnName.length() - 1); // Extracts the name inside "Column(...)"
    }
    return columnName;
}


/**
 * Safely cast an object to Comparable.
 *
 * @param value the object to cast
 * @return the casted Comparable object
 * @throws ClassCastException if the object is not a Comparable
 */
private Comparable<Object> castToComparable(Object value) {
    if (value instanceof Comparable) {
        return (Comparable<Object>) value;
    }
    throw new ClassCastException("Value is not comparable: " + value);
}

private <T extends Comparable<T>> boolean evaluateFilterAgainstStats(
        Expression filterExpression,
        Map<String, Object> minValues,
        Map<String, Object> maxValues
) {
    if (filterExpression == null) {
        return true; // No filter, include all files
    }

    if (filterExpression instanceof EqualTo) {
        EqualTo equalTo = (EqualTo) filterExpression;

        String columnName = equalTo.children().get(0).toString(); // Column name
        Object value = equalTo.children().get(1).eval(null); // Literal value

	String normalizedColumnName = normalizeColumnName(columnName);

        // Get min and max values for the column
        Object minValue = minValues.get(normalizedColumnName);
        Object maxValue = maxValues.get(normalizedColumnName);

        if (minValue == null || maxValue == null || value == null) {
            LOG.info(String.format(
                "Skipping evaluation due to null values: Column=%s, Min=%s, Max=%s, Value=%s",
                normalizedColumnName, minValue, maxValue, value
            ));
            return false; // Cannot evaluate
        }

        // Normalize values
        T normalizedValue = normalizeValue(value);
        T normalizedMinValue = normalizeValue(minValue);
        T normalizedMaxValue = normalizeValue(maxValue);

        // Perform comparison
        return normalizedMinValue.compareTo(normalizedValue) <= 0 &&
               normalizedMaxValue.compareTo(normalizedValue) >= 0;

    } else if (filterExpression instanceof And) {
        And and = (And) filterExpression;
        return evaluateFilterAgainstStats(and.children().get(0), minValues, maxValues) &&
               evaluateFilterAgainstStats(and.children().get(1), minValues, maxValues);

    } else if (filterExpression instanceof Or) {
        Or or = (Or) filterExpression;
        return evaluateFilterAgainstStats(or.children().get(0), minValues, maxValues) ||
               evaluateFilterAgainstStats(or.children().get(1), minValues, maxValues);
    }

    throw new UnsupportedOperationException("Unsupported filter expression: " + filterExpression.toString());
}

@SuppressWarnings("unchecked")
private <T extends Comparable<T>> T normalizeValue(Object value) {
    if (value instanceof Integer) {
        return (T) Long.valueOf(((Integer) value).longValue()); // Convert Integer to Long
    } else if (value instanceof Long) {
        return (T) value; // Long is already Comparable
    } else if (value instanceof Double) {
        return (T) value; // Double is already Comparable
    } else if (value instanceof String) {
        return (T) value; // String is naturally Comparable
    } else if (value instanceof java.sql.Date || value instanceof java.util.Date) {
        return (T) Long.valueOf(((java.util.Date) value).getTime()); // Convert Date to timestamp (Long)
    } else if (value instanceof java.time.LocalDate) {
        return (T) Long.valueOf(java.sql.Date.valueOf((java.time.LocalDate) value).getTime()); // Convert LocalDate to timestamp
    } else {
        throw new IllegalArgumentException("Unsupported value type: " + (value != null ? value.getClass().getName() : "null"));
    }
}


private boolean shouldIncludeFile(AddFile file, Expression deltaExpression) {
    if (deltaExpression == null) {
        return true; // No filter, include all files
    }

    try {
        String stats = file.getStats();
        if (stats == null || stats.isEmpty()) {
            LOG.info("File: " + file.getPath() + " has no statistics, including by default.");
            return true;
        }

        // Parse min and max values from file statistics
        Map<String, Object> minValues = extractMinValues(stats);
        Map<String, Object> maxValues = extractMaxValues(stats);

        LOG.info("File: " + file.getPath() + " Min Values: " + minValues);
        LOG.info("File: " + file.getPath() + " Max Values: " + maxValues);

        // Evaluate the filter against the statistics
        boolean result = evaluateFilterAgainstStats(deltaExpression, minValues, maxValues);
        LOG.info("File: " + file.getPath() + " matches filter: " + result);
        return result;
    } catch (Exception e) {
        LOG.log(Level.SEVERE, "Error evaluating filter for file: " + file.getPath(), e);
        return false; // Exclude the file if there's an error
    }
}


private List<AddFile> collectFiles(CloseableIterator<AddFile> iterator) {
    List<AddFile> files = new ArrayList<>();
    try {
        while (iterator.hasNext()) {
            files.add(iterator.next());
        }
    } finally {
        try {
            iterator.close();
        } catch (IOException e) {
		LOG.log(Level.SEVERE, "Error message here", e);

        }
    }
    return files;
}




    private boolean openNextFile() {
    closeCurrentRowIterator(); // Ensure the current file is closed properly
    synchronized (fileIterator) { // Add synchronization to prevent overlapping
        if (fileIterator.hasNext()) {
            try {
                String filePath = fileIterator.next();
                LOG.info("Processing file: " + filePath);
                rowIterator = snapshot.open(); // Open the iterator for the current file
                return true;
            } catch (Exception e) {
                LOG.log(Level.SEVERE, "Error opening file for reading rows", e);
            }
        }
    }
    return false; // No more files to process
}

@Override
public OneRow readNextObject() {
    try {
        final Instant start = Instant.now(); // Start time tracking
        List<RowRecord> batch = new ArrayList<>();

        // Collect a batch of rows
        while (rowIterator != null && rowIterator.hasNext() && batch.size() < batchSize) {
            batch.add(rowIterator.next());
        }

        if (!batch.isEmpty()) {
            //final long elapsedMillis = Duration.between(start, Instant.now()).toMillis();
            //LOG.info(String.format("Read batch of %d rows in %d milliseconds", batch.size(), elapsedMillis));
            return new OneRow(null, batch); // Return the batch
        }

        // If no rows are left in the current file, move to the next file
        if (openNextFile()) {
            LOG.info("Switching to the next file for processing.");
            return readNextObject(); // Recursive call to process the next file
        }
    } catch (Exception e) {
        LOG.log(Level.SEVERE, "Error reading next batch of objects", e);
    }

    return null; // No more data available
}


    @Override
    public void closeForRead() {
        LOG.info("Closing DeltaTableAccessor.");
        closeCurrentRowIterator();
    }


    private void closeCurrentRowIterator() {
    if (rowIterator != null) {
        try {
            rowIterator.close();
        } catch (IOException e) {
            LOG.log(Level.WARNING, "Error closing row iterator", e);
        }
        rowIterator = null;
    }
}

private Map<String, Object> extractMinValues(String stats) throws IOException {
    ObjectMapper objectMapper = new ObjectMapper();
    JsonNode statsNode = objectMapper.readTree(stats);
    return objectMapper.convertValue(statsNode.get("minValues"), new TypeReference<Map<String, Object>>() {});
}

private Map<String, Object> extractMaxValues(String stats) throws IOException {
    ObjectMapper objectMapper = new ObjectMapper();
    JsonNode statsNode = objectMapper.readTree(stats);
    return objectMapper.convertValue(statsNode.get("maxValues"), new TypeReference<Map<String, Object>>() {});
}


    @Override
    public boolean openForWrite() {
        throw new UnsupportedOperationException("Write operations are not supported by DeltaTableAccessor");
    }

    @Override
    public boolean writeNextObject(OneRow oneRow) {
        throw new UnsupportedOperationException("Write operations are not supported by DeltaTableAccessor");
    }

    @Override
    public void closeForWrite() {
        throw new UnsupportedOperationException("Write operations are not supported by DeltaTableAccessor");
    }

    private Configuration initializeHadoopConfiguration() {
        Configuration hadoopConf = new Configuration();
        hadoopConf.set("fs.defaultFS", "file:///");
        hadoopConf.set("mapreduce.framework.name", "local");
        hadoopConf.set("hadoop.tmp.dir", "/tmp/hadoop");
        LOG.info("Hadoop configuration initialized.");
        return hadoopConf;
    }

    private String extractRowValues(RowRecord row, List<StructField> fields) {
        StringBuilder rowValues = new StringBuilder();

        for (StructField field : fields) {
            String fieldName = field.getName();
            Object value;

            // Handle field types
            switch (field.getDataType().getTypeName().toLowerCase()) {
                case "integer":
                    value = row.getInt(fieldName);
                    break;
                case "double":
                    value = row.getDouble(fieldName);
                    break;
                case "string":
                    value = row.getString(fieldName);
                    break;
                case "boolean":
                    value = row.getBoolean(fieldName);
                    break;
                case "long":
                    value = row.getLong(fieldName);
                    break;
                case "date":
                    value = row.getDate(fieldName);
                    break;
                case "timestamp":
                    value = row.getTimestamp(fieldName);
                    break;
                case "decimal":
                    value = row.getBigDecimal(fieldName);
                    break;
                default:
                    value = "Unsupported Type";
            }

            rowValues.append(fieldName).append("=").append(value).append(", ");
        }

        if (rowValues.length() > 0) {
            rowValues.setLength(rowValues.length() - 2); // Remove trailing comma and space
        }

        return rowValues.toString();
    }
}

