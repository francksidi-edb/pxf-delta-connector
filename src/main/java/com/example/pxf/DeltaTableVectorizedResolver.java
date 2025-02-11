package com.example.pxf;



import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.*;
import io.delta.kernel.types.*;

import org.apache.hadoop.conf.Configuration;

import org.greenplum.pxf.api.OneField;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.model.ReadVectorizedResolver;
import org.greenplum.pxf.api.model.Resolver;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

public class DeltaTableVectorizedResolver implements ReadVectorizedResolver, Resolver {

    private static final Logger LOG = Logger.getLogger(DeltaTableVectorizedResolver.class.getName());
    private static final String BATCH_SIZE_PROPERTY = "batch_size";
    private static final int DEFAULT_BATCH_SIZE = 1024;

    private RequestContext context;
    private Snapshot snapshot;
    private StructType schema;
    private int batchSize;

    @Override
    public void setRequestContext(RequestContext context) {
        this.context = context;
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
        Engine engine = DefaultEngine.create(hadoopConf);
        Table deltaTable = Table.forPath(engine, context.getDataSource());
        snapshot = deltaTable.getLatestSnapshot(engine);

        schema = snapshot.getSchema(engine);
        if (schema == null) {
            throw new IllegalStateException("Schema is null. Ensure the Delta table has valid metadata.");
        }

        LOG.info("Schema successfully loaded: " + schema.toString());
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

        try {
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
        List<Row> records = (List<Row>) rowData;

        LOG.info(String.format("Processing batch of %d rows", records.size()));

        final Instant start = Instant.now();

        List<List<OneField>> resolvedBatch = new ArrayList<>(records.size());
        for (Row record : records) {
            resolvedBatch.add(processRecord(record));
        }

        final long elapsedMillis = Duration.between(start, Instant.now()).toMillis();
        LOG.info(String.format("Processed batch of %d rows in %d milliseconds", records.size(), elapsedMillis));

        return resolvedBatch;
    }

    private List<OneField> processRecord(Row row) {
        List<ColumnDescriptor> tupleDescription = context.getTupleDescription();
        List<OneField> fields = new ArrayList<>(tupleDescription.size());
        int i = 0;

        for (ColumnDescriptor column : tupleDescription) {
            Object value = extractValue(row, i);
            fields.add(new OneField(column.getDataType().getOID(), value));
            i++;
        }

        return fields;
    }

    private Object extractValue(Row row, int columnOrdinal) {
        if (schema == null) {
            throw new IllegalStateException("Schema is null. Ensure schema is initialized before extracting values.");
        }

        DataType dataType = row.getSchema().at(columnOrdinal).getDataType();

        if (row.isNullAt(columnOrdinal)) {
            return null;
        } else if (dataType instanceof BooleanType) {
            return row.getBoolean(columnOrdinal);
        } else if (dataType instanceof ByteType) {
            return row.getByte(columnOrdinal);
        } else if (dataType instanceof ShortType) {
            return row.getShort(columnOrdinal);
        } else if (dataType instanceof IntegerType) {
            return row.getInt(columnOrdinal);
        } else if (dataType instanceof DateType) {
            // DateType data is stored internally as the number of days since 1970-01-01
            int daysSinceEpochUTC = row.getInt(columnOrdinal);
            return LocalDate.ofEpochDay(daysSinceEpochUTC).toString();
        } else if (dataType instanceof LongType) {
            return row.getLong(columnOrdinal);
        } else if (dataType instanceof TimestampType || dataType instanceof TimestampNTZType) {
            // Timestamps are stored internally as the number of microseconds since epoch.
            // TODO: TimestampType should use the session timezone to display values.
            long microSecsSinceEpochUTC = row.getLong(columnOrdinal);
            LocalDateTime dateTime = LocalDateTime.ofEpochSecond(
                microSecsSinceEpochUTC / 1_000_000 /* epochSecond */,
                (int) (1000 * microSecsSinceEpochUTC % 1_000_000) /* nanoOfSecond */,
                ZoneOffset.UTC);
            return dateTime.toString();
        } else if (dataType instanceof FloatType) {
            return row.getFloat(columnOrdinal);
        } else if (dataType instanceof DoubleType) {
            return row.getDouble(columnOrdinal);
        } else if (dataType instanceof StringType) {
            return row.getString(columnOrdinal);
        } else if (dataType instanceof BinaryType) {
            return new String(row.getBinary(columnOrdinal));
        } else if (dataType instanceof DecimalType) {
            return row.getDecimal(columnOrdinal);
        } else {
            throw new UnsupportedOperationException("unsupported data type: " + dataType);
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


