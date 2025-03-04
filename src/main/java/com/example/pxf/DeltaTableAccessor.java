package com.example.pxf;

import org.apache.hadoop.conf.Configuration;
import org.greenplum.pxf.api.model.BasePlugin;

import com.example.pxf.partitioning.DeltaFragmentMetadata;

import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;

import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.*;
import java.io.IOException;
import java.io.File;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import org.apache.commons.lang3.StringUtils;

import io.delta.kernel.*;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.types.*;
import io.delta.kernel.internal.InternalScanFileUtils;
import io.delta.kernel.internal.data.ScanStateRow;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.defaults.internal.data.DefaultColumnarBatch;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.utils.*;
import io.delta.kernel.expressions.*;

import static io.delta.kernel.internal.util.Utils.singletonCloseableIterator;
import static io.delta.kernel.internal.util.Utils.toCloseableIterator;

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
    private Predicate recordFilter;
    private StructType tableSchema;
    private TransactionBuilder txnBuilder;
    private List<FilteredColumnarBatch> filteredBatches = new ArrayList<>();
    private List<Row> dataActions = new ArrayList<>();
    private Transaction txn;
    private boolean isParentPath = false;
    private int rowCounter = 0;

    @Override
    public void afterPropertiesSet() {
        try {
            Configuration hadoopConf = initializeHadoopConfiguration();
            engine = DefaultEngine.create(hadoopConf);
        } catch (Exception e) {
            LOG.log(Level.SEVERE, "Error initialize Delta table", e);
        }
    }

    @Override
    public boolean openForRead() {
        try {
            String tablePath = context.getDataSource();
            fragmentMeta = context.getFragmentMetadata();
            isParentPath = DeltaUtilities.isParentPath(tablePath);
            if (isParentPath) {
                String partitionInfo = fragmentMeta.getPartitionInfo();
                if (partitionInfo == null || partitionInfo.isEmpty()) {
                    LOG.info("Partition filter is empty for parent path: " + tablePath);
                } else {
                    tablePath = tablePath + "/" + partitionInfo;
                }
            }
            LOG.info("Opening DeltaLog for table path: " + tablePath);

            Table deltaTable = Table.forPath(engine, tablePath);
            snapshot = deltaTable.getLatestSnapshot(engine);
            ScanBuilder scanBuilder = snapshot.getScanBuilder(engine);
            StructType readSchema = DeltaUtilities.getReadSchema(context.getTupleDescription(), snapshot.getPartitionColumnNames(engine));
            readSchema.fields().forEach(f -> LOG.info("Field: " + f.getName() + " Type: " + f.getDataType()));
            scanBuilder = scanBuilder.withReadSchema(engine, readSchema);
            recordFilter = DeltaUtilities.getFilterPredicate(context.getFilterString(), readSchema, context.getTupleDescription());
            LOG.info("Filter predicate: " + recordFilter);
            if (recordFilter != null) {
                scanBuilder = scanBuilder.withFilter(engine, recordFilter);
            }
            scan = scanBuilder.build();

            scanState = scan.getScanState(engine);
            scanFileIter = scan.getScanFiles(engine);

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
                if (!isParentPath && !filePath.contains(fragmentMeta.getFilePath())) {
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
                  (recordFilter == null) ? Optional.empty() : Optional.of(recordFilter)  /* optional predicate the connector can apply to filter data from the reader */
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
                rowCounter++;
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
        LOG.info("Closing DeltaTableAccessor. Total rows read: " + rowCounter);
        rowCounter = 0;
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

    private void createTransaction(String tablePath) {
        // Create a `Table` object with the given destination table path
        Table table = Table.forPath(engine, tablePath);

        // Create a transaction builder to build the transaction
        try {
            LOG.info("Creating transaction for table: " + tablePath);
            txnBuilder =
                table.createTransactionBuilder(
                        engine,
                        "pxf", /* engineInfo */
                        Operation.CREATE_TABLE);

            tableSchema = DeltaUtilities.generateParquetSchema(context.getTupleDescription());
            if (!DeltaUtilities.isDeltaTable(tablePath)) {
                // Set the schema of the NEW table on the transaction builder
                txnBuilder = txnBuilder.withSchema(engine, tableSchema);
            }
            // Build the transaction
            txn = txnBuilder.build(engine);
            //LOG.info("thread name: " + Thread.currentThread().getName() + " txn version: " + context.getSegmentId());
        } catch (Exception e) {
            LOG.log(Level.SEVERE, "Error creating transaction", e);
        }
    }

    @Override
    public boolean openForWrite() {
        String tablePath = String.format("%s/%s_%d", StringUtils.removeEnd(context.getDataSource(), "/"), "seg", context.getSegmentId());
        LOG.info("open for write, table path: " + tablePath);
        createTransaction(tablePath);

        return true;
    }

    private void verifyCommitSuccess(String tablePath, TransactionCommitResult result) {
        // Verify the commit was successful
        if (result.getVersion() >= 0) {
            LOG.info("Table created/commited successfully at: " + tablePath);
        } else {
            // This should never happen. If there is a reason for table be not created
            // `Transaction.commit` always throws an exception.
            throw new RuntimeException("Table creation/commit failed");
        }
    }

    private void writeDataToParquetFiles(CloseableIterator<FilteredColumnarBatch> data) {
        try {
            // Get the transaction state
            Row txnState = txn.getTransactionState(engine);
            // First transform the logical data to physical data that needs to be written to the Parquet
            // files
            CloseableIterator<FilteredColumnarBatch> physicalData =
                    Transaction.transformLogicalData(
                            engine,
                            txnState,
                            data,
                            // partition values - as this table is unpartitioned, it should be empty
                            Collections.emptyMap());
            // Get the write context
            DataWriteContext writeContext = Transaction.getWriteContext(
                    engine,
                    txnState,
                    // partition values - as this table is unpartitioned, it should be empty
                    Collections.emptyMap());

            // Write the physical data to Parquet files
            CloseableIterator<DataFileStatus> dataFiles = engine.getParquetHandler()
                    .writeParquetFiles(
                            writeContext.getTargetDirectory(),
                            physicalData,
                            writeContext.getStatisticsColumns());
            // Now convert the data file status to data actions that needs to be written to the Delta
            // table log
            CloseableIterator<Row> dataAction = Transaction.generateAppendActions(engine, txnState, dataFiles, writeContext);
            while (dataAction.hasNext()) {
                dataActions.add(dataAction.next());
            }
        } catch (Exception e) {
            LOG.log(Level.SEVERE, "Error writing data to Parquet files", e);
        }
    }

    @Override
    public boolean writeNextObject(OneRow oneRow) {
        ColumnVector[] columns = (ColumnVector[]) oneRow.getData();
        ColumnarBatch batch = new DefaultColumnarBatch(1, tableSchema, columns);
        FilteredColumnarBatch filteredBatch = new FilteredColumnarBatch(batch, Optional.empty());
        filteredBatches.add(filteredBatch);
        if (filteredBatches.size() >= 1024) {
            writeDataToParquetFiles(toCloseableIterator(filteredBatches.iterator()));
            filteredBatches.clear();
        }

        return true;
    }

    private synchronized void commitTx(){
        try {
            // Create a iterable out of the data actions. If the contents are too big to fit in memory,
            // the connector may choose to write the data actions to a temporary file and return an
            // iterator that reads from the file.
            CloseableIterable<Row> dataActionsIterable =
            CloseableIterable.inMemoryIterable(toCloseableIterator(dataActions.iterator()));
            // Commit the transaction.
            TransactionCommitResult commitResult = txn.commit(engine, dataActionsIterable);
                    // Check the transaction commit result
            verifyCommitSuccess(context.getDataSource(), commitResult);
            } catch (Exception e) {
                LOG.log(Level.SEVERE, "Error commit tx", e);
                return ;
        }
    }

    @Override
    public void closeForWrite() {
        if (filteredBatches.size() > 0) {
            writeDataToParquetFiles(toCloseableIterator(filteredBatches.iterator()));
            filteredBatches.clear();
        }
        commitTx();
        return;
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

