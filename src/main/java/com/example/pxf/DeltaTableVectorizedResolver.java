package com.example.pxf;

import io.delta.standalone.DeltaLog;
import io.delta.standalone.Snapshot;
import io.delta.standalone.data.RowRecord;
import io.delta.standalone.types.StructField;
import io.delta.standalone.types.StructType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetReader;
import org.greenplum.pxf.api.OneField;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.model.ReadVectorizedResolver;
import org.greenplum.pxf.api.model.Resolver;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import com.example.pxf.parquet.RowReadSupport;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import java.util.stream.Collectors;
import java.time.Duration;
import java.time.Instant;

public class DeltaTableVectorizedResolver implements ReadVectorizedResolver, Resolver {

    private static final Logger LOG = Logger.getLogger(DeltaTableVectorizedResolver.class.getName());
    private static final String BATCH_SIZE_PROPERTY = "batch_size";
    private static final int DEFAULT_BATCH_SIZE = 1024;

    private RequestContext context;
    private DeltaLog deltaLog;
    private Snapshot snapshot;
    private StructType schema;
    private Configuration configuration;
    private int batchSize;
    private Map<String, StructField> fieldMap;

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


    private void initialize() {
        LOG.info("Initializing DeltaTableVectorizedResolver...");

        String fullPath = context.getDataSource();
        if (fullPath == null || fullPath.isEmpty()) {
            throw new IllegalArgumentException("Delta table path is not provided.");
        }

        String rootPath = getRootPath(fullPath);

        try {
            Configuration hadoopConf = initializeHadoopConfiguration();
            deltaLog = DeltaLog.forTable(hadoopConf, new Path(rootPath));
            snapshot = deltaLog.snapshot();
            schema = snapshot.getMetadata().getSchema();

            if (schema == null) {
                throw new IllegalStateException("Schema is null. Ensure the Delta table has valid metadata.");
            }

            LOG.info("Schema successfully loaded from root path: " + schema.getTreeString());
            initializeFieldMap();

            String batchSizeString = context.getOption(BATCH_SIZE_PROPERTY);
            batchSize = (batchSizeString != null) ? Integer.parseInt(batchSizeString) : DEFAULT_BATCH_SIZE;
            LOG.info("Batch size initialized to: " + batchSize);
        } catch (Exception e) {
            LOG.log(Level.SEVERE, "Error initializing DeltaTableVectorizedResolver", e);
            throw new RuntimeException("Failed to initialize DeltaTableVectorizedResolver", e);
        }
    }

    private Configuration initializeHadoopConfiguration() {
        Configuration hadoopConf = new Configuration();
        hadoopConf.set("fs.defaultFS", "file:///");
        hadoopConf.set("mapreduce.framework.name", "local");
        hadoopConf.set("hadoop.tmp.dir", System.getProperty("java.io.tmpdir") + "/hadoop");

        LOG.info("Hadoop configuration initialized.");
        return hadoopConf;
    }

    private String getRootPath(String fullPath) {
        int partitionIndex = fullPath.indexOf("/year=");
        return (partitionIndex == -1) ? fullPath : fullPath.substring(0, partitionIndex);
    }

    private void initializeFieldMap() {
        fieldMap = Arrays.stream(schema.getFields())
                .collect(Collectors.toMap(StructField::getName, field -> field));
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

        LOG.info(String.format("Processing batch of %d rows", records.size()));

        final Instant start = Instant.now();

        List<List<OneField>> resolvedBatch = new ArrayList<>(records.size());
        for (RowRecord record : records) {
            resolvedBatch.add(processRecord(record));
        }

        final long elapsedMillis = Duration.between(start, Instant.now()).toMillis();
        LOG.info(String.format("Processed batch of %d rows in %d milliseconds", records.size(), elapsedMillis));

        return resolvedBatch;
    }

    private List<OneField> processRecord(RowRecord record) {
        List<ColumnDescriptor> tupleDescription = context.getTupleDescription();
        List<OneField> fields = new ArrayList<>(tupleDescription.size());

        for (ColumnDescriptor column : tupleDescription) {
            String columnName = column.columnName();
            Object value = extractValue(record, columnName);
            fields.add(new OneField(column.getDataType().getOID(), value));
        }

        return fields;
    }

    private Object extractValue(RowRecord record, String columnName) {
        if (schema == null) {
            throw new IllegalStateException("Schema is null. Ensure schema is initialized before extracting values.");
        }

        if (fieldMap == null) {
            initializeFieldMap();
        }

        StructField field = fieldMap.get(columnName);
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
                return record.getDate(columnName).toString();
            case "timestamp":
                return record.getTimestamp(columnName).toString();
            case "decimal":
                return record.getBigDecimal(columnName);
            default:
                throw new UnsupportedOperationException("Unsupported column type: " + field.getDataType().getTypeName());
        }
    }

@Override
    public List<OneField> getFields(OneRow row) {
        throw new UnsupportedOperationException("getFields is not supported for this resolver.");
    }

    @Override
    public OneRow setFields(List<OneField> record) {
        throw new UnsupportedOperationException("Writing is not supported by DeltaTableVectorizedResolver.");
    }

}


