package com.example.pxf;



import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.defaults.internal.data.vector.DefaultGenericVector;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.*;
import io.delta.kernel.types.*;

import org.apache.hadoop.conf.Configuration;
import org.greenplum.pxf.api.OneField;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.error.PxfRuntimeException;
import org.greenplum.pxf.api.error.UnsupportedTypeException;
import org.greenplum.pxf.api.model.ReadVectorizedResolver;
import org.greenplum.pxf.api.model.WriteVectorizedResolver;
import org.greenplum.pxf.api.model.Resolver;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

public class DeltaTableVectorizedResolver implements ReadVectorizedResolver, WriteVectorizedResolver, Resolver {

    private static final Logger LOG = Logger.getLogger(DeltaTableVectorizedResolver.class.getName());
    private static final String BATCH_SIZE_PROPERTY = "batch_size";
    private static final int DEFAULT_BATCH_SIZE = 1024;

    private RequestContext context;
    private int batchSize;

    @Override
    public void setRequestContext(RequestContext context) {
        this.context = context;
        initialize();
    }


    @Override
    public void afterPropertiesSet() {
        LOG.info("Initializing DeltaTableResolverVec...");
    }

    private void initialize() {
        LOG.info("Initializing DeltaTableVectorizedResolver...");

        try {
            String batchSizeString = context.getOption(BATCH_SIZE_PROPERTY);
            batchSize = (batchSizeString != null) ? Integer.parseInt(batchSizeString) : DEFAULT_BATCH_SIZE;
            LOG.info("Batch size initialized to: " + batchSize);
        } catch (Exception e) {
            LOG.log(Level.SEVERE, "Error initializing DeltaTableVectorizedResolver", e);
            throw new RuntimeException("Failed to initialize DeltaTableVectorizedResolver", e);
        }
    }

    @Override
    public List<List<OneField>> getFieldsForBatch(OneRow batch) {
        Object rowData = batch.getData();
        if (!(rowData instanceof List)) {
            throw new IllegalArgumentException("Batch data type is not supported: " +
                    (rowData != null ? rowData.getClass().getName() : "null"));
        }

        @SuppressWarnings("unchecked")
        List<Row> records = (List<Row>) rowData;

        LOG.fine(String.format("Processing batch of %d rows", records.size()));

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

    @Override
    public int getBatchSize() {
        return batchSize;
    }

    private ColumnVector convertToColumnVector(OneField field[], int index) {
        List<ColumnDescriptor> columnDescriptors = context.getTupleDescription();
        ColumnDescriptor columnDescriptor = columnDescriptors.get(index);
        org.greenplum.pxf.api.io.DataType columnType = columnDescriptor.getDataType();

        switch (columnType) {
            case SMALLINT:
                Short[] shortArray = new Short[field.length];
                for (int i = 0; i < field.length; i++) {
                    shortArray[i] = (short) field[i].val;
                }
                return DefaultGenericVector.fromArray(ShortType.SHORT, shortArray); 
            case INTEGER:
                Integer[] intArray = new Integer[field.length];
                for (int i = 0; i < field.length; i++) {
                    intArray[i] = (int) field[i].val;
                }
                return DefaultGenericVector.fromArray(IntegerType.INTEGER, intArray);
            case BIGINT:
                Long[] longArray = new Long[field.length];
                for (int i = 0; i < field.length; i++) {
                    longArray[i] = (long) field[i].val;
                }
                return DefaultGenericVector.fromArray(LongType.LONG, longArray);
            case REAL:
                Float[] floatArray = new Float[field.length];
                for (int i = 0; i < field.length; i++) {
                    floatArray[i] = (float) field[i].val;
                }
                return DefaultGenericVector.fromArray(FloatType.FLOAT, floatArray);
            case FLOAT8:
                Double[] doubleArray = new Double[field.length];
                for (int i = 0; i < field.length; i++) {
                    doubleArray[i] = (double) field[i].val;
                }
                return DefaultGenericVector.fromArray(DoubleType.DOUBLE, doubleArray);
            case TEXT:
            case VARCHAR:
                String[] stringArray = new String[field.length];
                for (int i = 0; i < field.length; i++) {
                    stringArray[i] = (String) field[i].val;
                }
                return DefaultGenericVector.fromArray(StringType.STRING, stringArray);
            case BOOLEAN:
                Boolean[] booleanArray = new Boolean[field.length];
                for (int i = 0; i < field.length; i++) {
                    booleanArray[i] = (boolean) field[i].val;
                }
                return DefaultGenericVector.fromArray(BooleanType.BOOLEAN, booleanArray);
            case BYTEA:
                Byte[] byteArray = new Byte[field.length];
                for (int i = 0; i < field.length; i++) {
                    byteArray[i] = (byte) field[i].val;
                }
                return DefaultGenericVector.fromArray(BinaryType.BINARY, byteArray);
            case DATE:
                Integer[] dateArray = new Integer[field.length];
                for (int i = 0; i < field.length; i++) {
                    java.sql.Date date = java.sql.Date.valueOf(field[i].val.toString());
                    long day = TimeUnit.SECONDS.toDays(date.getTime()/1000);    //Todo: handle timezone?
                    dateArray[i] = (int)day;
                }
                return DefaultGenericVector.fromArray(io.delta.kernel.types.DateType.DATE, dateArray);
            case TIMESTAMP:
                Long[] timestampArray = new Long[field.length];
                for (int i = 0; i < field.length; i++) {
                    Instant instant = java.sql.Timestamp.valueOf(field[i].val.toString()).toInstant();
                    long micros = TimeUnit.SECONDS.toMicros(instant.getEpochSecond()) + TimeUnit.NANOSECONDS.toMicros(instant.getNano());
                    timestampArray[i] = (Long) micros;
                }
                return DefaultGenericVector.fromArray(io.delta.kernel.types.TimestampType.TIMESTAMP, timestampArray);
            case NUMERIC:
                String[] numericArray = new String[field.length];
                for (int i = 0; i < field.length; i++) {
                    numericArray[i] = (String) field[i].val;
                }
                return DefaultGenericVector.fromArray(io.delta.kernel.types.StringType.STRING, numericArray);
            default:
                throw new UnsupportedTypeException(columnType.name());
        }
    }

    @Override
    public OneRow setFieldsForBatch(List<List<OneField>> records) {
        if (records == null || records.isEmpty()) {
            return null; // this will end bridge iterations
        }
        // make sure provided record set can fit into a single batch, we do not want to produce multiple batches here
        if (records.size() > getBatchSize()) {
            throw new PxfRuntimeException(String.format("Provided set of %d records is greater than the batch size of %d",
                    records.size(), getBatchSize()));
        }

        LOG.info(String.format("Processing batch of %d rows to write", records.size()));
        // iterate over incoming rows
        int rowIndex = 0;
        
        ColumnVector[] vectors = new ColumnVector[context.getTupleDescription().size()];
        OneField[][] fields = new OneField[vectors.length][records.size()];
        for (List<OneField> record : records) {
            int columnIndex = 0;
            // fill up column fields for the columns of the given row with record values using mapping functions
            for (OneField field : record) {
                fields[columnIndex][rowIndex] = field;
                // Todo: handle null values
                columnIndex++;
            }
            rowIndex++;
        }
        for (int i = 0; i < vectors.length; i++) {
            vectors[i] = convertToColumnVector(fields[i], i);
        }

        return new OneRow(vectors);
    }
    private Configuration initializeHadoopConfiguration() {
        Configuration hadoopConf = new Configuration();
        hadoopConf.set("fs.defaultFS", "file:///");
        hadoopConf.set("mapreduce.framework.name", "local");
        hadoopConf.set("hadoop.tmp.dir", "/tmp/hadoop");
        LOG.info("Hadoop configuration initialized.");
        return hadoopConf;
    }
}


