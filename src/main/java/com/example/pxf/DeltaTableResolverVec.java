package com.example.pxf;

import io.delta.standalone.Snapshot;
import io.delta.standalone.data.RowRecord;
import io.delta.standalone.types.StructField;
import io.delta.standalone.types.StructType;
import org.greenplum.pxf.api.OneField;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.model.ReadVectorizedResolver;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import io.delta.standalone.DeltaLog;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

/**
 * Resolver for Delta Table rows into Greenplum fields with batch processing.
 */
public class DeltaTableResolver implements ReadVectorizedResolver {

    private static final Logger LOG = Logger.getLogger(DeltaTableResolver.class.getName());
    private static final int DEFAULT_BATCH_SIZE = 1000;

    private RequestContext context;
    private DeltaLog deltaLog;
    private Snapshot snapshot;
    private StructType schema;
    private Configuration configuration;
    private int batchSize;

    public void setRequestContext(RequestContext context) {
        this.context = context;
        this.configuration = context.getConfiguration();
        initialize();
    }

    private void initialize() {
        LOG.info("Initializing DeltaTableResolver...");

        String deltaTablePath = context.getDataSource();
        if (deltaTablePath == null || deltaTablePath.isEmpty()) {
            throw new IllegalArgumentException("Delta table path is not provided.");
        }

        try {
            Configuration hadoopConf = new Configuration();
            hadoopConf.set("fs.defaultFS", "file:///");
            hadoopConf.set("mapreduce.framework.name", "local");
            hadoopConf.set("hadoop.tmp.dir", System.getProperty("java.io.tmpdir") + "/hadoop");

            String batchSizeString = configuration.get("delta.batch.size");
            batchSize = (batchSizeString != null) ? Integer.parseInt(batchSizeString) : DEFAULT_BATCH_SIZE;

            deltaLog = DeltaLog.forTable(hadoopConf, new Path(deltaTablePath));
            snapshot = deltaLog.snapshot();
            schema = snapshot.getMetadata().getSchema();

            if (schema == null) {
                throw new IllegalStateException("Schema is null. Ensure the Delta table has valid metadata.");
            }

            LOG.info("Schema successfully loaded: " + schema.getTreeString());
        } catch (Exception e) {
            LOG.severe("Error initializing DeltaTableResolver: " + e.getMessage());
            throw new RuntimeException("Failed to initialize DeltaTableResolver", e);
        }
    }

    @Override
    public List<List<OneField>> getFieldsForBatch(OneRow batch) {
        if (schema == null) {
            throw new IllegalStateException("Schema is not initialized. Check Delta table metadata.");
        }

        List<List<OneField>> resolvedBatch = new ArrayList<>();
        Object rowData = batch.getData();

        if (rowData instanceof List) {
            List<RowRecord> records = (List<RowRecord>) rowData;
            for (RowRecord record : records) {
                resolvedBatch.add(processRecord(record));
            }
        } else {
            throw new IllegalArgumentException("Batch data type is not supported: " + rowData.getClass());
        }

        return resolvedBatch;
    }

    private List<OneField> processRecord(RowRecord record) {
        List<OneField> fields = new ArrayList<>();
        for (ColumnDescriptor column : context.getTupleDescription()) {
            String columnName = column.columnName();
            Object value = extractValue(record, columnName);

            OneField oneField = new OneField();
            oneField.type = column.getDataType().getOID();
            oneField.val = value;

            fields.add(oneField);
        }
        return fields;
    }

    private Object extractValue(RowRecord record, String columnName) {
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
                return record.getDate(columnName).toString();
            case "timestamp":
                return record.getTimestamp(columnName).toString();
            default:
                throw new UnsupportedOperationException("Unsupported column type: " + field.getDataType().getTypeName());
        }
    }

    public OneRow setFields(List<OneField> record) {
        throw new UnsupportedOperationException("Writing is not supported by DeltaTableResolver.");
    }
}


