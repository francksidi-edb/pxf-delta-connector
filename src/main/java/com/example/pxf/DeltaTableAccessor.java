package com.example.pxf;

import org.apache.hadoop.conf.Configuration;
import org.greenplum.pxf.api.model.BasePlugin;

import com.example.pxf.partitioning.DeltaFragmentMetadata;

import org.greenplum.pxf.api.OneRow;

import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.Optional;
import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

import io.delta.kernel.*;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.types.*;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import io.delta.kernel.internal.InternalScanFileUtils;
import io.delta.kernel.internal.data.ScanStateRow;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;

import static io.delta.kernel.internal.util.Utils.singletonCloseableIterator;

public class DeltaTableAccessor extends BasePlugin implements org.greenplum.pxf.api.model.Accessor {

    private static final Logger LOG = Logger.getLogger(DeltaTableAccessor.class.getName());
    private Engine engine;
    private Snapshot snapshot;
    private Scan scan;
    private CloseableIterator<FilteredColumnarBatch> scanFileIter;
    private CloseableIterator<FilteredColumnarBatch> transformedData;
    private CloseableIterator<Row> rowIterator;
    private CloseableIterator<Row> scanFileRows;
    private DeltaFragmentMetadata fragmentMeta;
    private Row scanState ;

    @Override
    public void afterPropertiesSet() {
        try {
            fragmentMeta = context.getFragmentMetadata();
            Configuration hadoopConf = initializeHadoopConfiguration();
            engine = DefaultEngine.create(hadoopConf);
            Table deltaTable = Table.forPath(engine, context.getDataSource());
            snapshot = deltaTable.getLatestSnapshot(engine);
            scan = snapshot.getScanBuilder(engine).build();

            scanState = scan.getScanState(engine);
            scanFileIter = scan.getScanFiles(engine);
        } catch (Exception e) {
            LOG.log(Level.SEVERE, "Error initialize Delta table", e);
        }
    }

    @Override
    public boolean openForRead() {
        try {
            String deltaTablePath = context.getDataSource();
            LOG.info("Opening DeltaLog for table path: " + deltaTablePath);

            return openNextFile(); // Open the first file for reading rows
        } catch (Exception e) {
            LOG.log(Level.SEVERE, "Error opening Delta table for reading", e);
            return false;
        }
    }

    private boolean readNextFiltedData() {
        try {
            if (transformedData != null && transformedData.hasNext()) {
                FilteredColumnarBatch filteredData = transformedData.next();
                rowIterator = filteredData.getRows();
                return true;
            } else if (readNextPhysicalData()) {
                return true; // Recursively process the next file
            }
        } catch (Exception e) {
            LOG.log(Level.SEVERE, "Error reading next filteredData", e);
        }
        return false;
    }

    private boolean readNextPhysicalData() {
        try {
            if (scanFileRows != null && scanFileRows.hasNext()) {
                Row scanFileRow = scanFileRows.next();
                FileStatus fileStatus = InternalScanFileUtils.getAddFileStatus(scanFileRow);
                String filePath = fileStatus.getPath();
                if (!filePath.contains(fragmentMeta.getFilePath())) {
                    LOG.fine("Skip the phyFile " + fileStatus.getPath());
                    return readNextPhysicalData();
                }
                LOG.info("Processing file: " + filePath);
                StructType physicalReadSchema =
                ScanStateRow.getPhysicalDataReadSchema(engine, scanState);
                CloseableIterator<ColumnarBatch> physicalDataIter =
                engine.getParquetHandler().readParquetFiles(
                  singletonCloseableIterator(fileStatus),
                  physicalReadSchema,
                  Optional.empty() /* optional predicate the connector can apply to filter data from the reader */
                );
                transformedData = Scan.transformPhysicalData(engine, scanState, scanFileRow, physicalDataIter);
                return readNextFiltedData();
            } else if (openNextFile()) {
                return true; // Recursively process the next file
            }
        } catch (Exception e) {
            LOG.log(Level.SEVERE, "Error reading next physicalDataIter", e);
        }
        return false;
    }

    private boolean openNextFile() {
        closeCurrentRowIterator(); // Ensure any previously open iterator is closed
        if (scanFileIter.hasNext()) {
            try {
                FilteredColumnarBatch scanFilesBatch = scanFileIter.next();
                scanFileRows = scanFilesBatch.getRows();
                return readNextPhysicalData();
            } catch (Exception e) {
                LOG.log(Level.SEVERE, "Error opening file for reading rows", e);
            }
        }
        return false; // No more files to process
    }

    @Override
    public OneRow readNextObject() {
        try {
            if (rowIterator != null && rowIterator.hasNext()) {
                Row row = rowIterator.next();
                return new OneRow(null, extractRowValues(row));
            } else if (readNextFiltedData()) {
                return readNextObject(); // Recursively process the next DataRows
            }
        } catch (Exception e) {
            LOG.log(Level.SEVERE, "Error reading next object", e);
        }
        return null;
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

private static String getValue(Row row, int columnOrdinal) {
        DataType dataType = row.getSchema().at(columnOrdinal).getDataType();
        if (row.isNullAt(columnOrdinal)) {
            return null;
        } else if (dataType instanceof BooleanType) {
            return Boolean.toString(row.getBoolean(columnOrdinal));
        } else if (dataType instanceof ByteType) {
            return Byte.toString(row.getByte(columnOrdinal));
        } else if (dataType instanceof ShortType) {
            return Short.toString(row.getShort(columnOrdinal));
        } else if (dataType instanceof IntegerType) {
            return Integer.toString(row.getInt(columnOrdinal));
        } else if (dataType instanceof DateType) {
            // DateType data is stored internally as the number of days since 1970-01-01
            int daysSinceEpochUTC = row.getInt(columnOrdinal);
            return LocalDate.ofEpochDay(daysSinceEpochUTC).toString();
        } else if (dataType instanceof LongType) {
            return Long.toString(row.getLong(columnOrdinal));
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
            return Float.toString(row.getFloat(columnOrdinal));
        } else if (dataType instanceof DoubleType) {
            return Double.toString(row.getDouble(columnOrdinal));
        } else if (dataType instanceof StringType) {
            return row.getString(columnOrdinal);
        } else if (dataType instanceof BinaryType) {
            return new String(row.getBinary(columnOrdinal));
        } else if (dataType instanceof DecimalType) {
            return row.getDecimal(columnOrdinal).toString();
        } else {
            throw new UnsupportedOperationException("unsupported data type: " + dataType);
        }
    }

    private String extractRowValues(Row row) {
        int numCols = row.getSchema().length();
        String fieldName = "";
        StringBuilder rowValues = new StringBuilder();
        for (int i = 0; i < numCols; i++) {
            fieldName = row.getSchema().fieldNames().get(i);
            rowValues.append(fieldName).append("=").append(getValue(row, i)).append(", ");
        }
        if (rowValues.length() > 0) {
            rowValues.setLength(rowValues.length() - 2); // Remove trailing comma and space
        }

        return rowValues.toString();
    }
}

