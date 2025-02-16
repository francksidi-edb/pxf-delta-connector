package com.example.pxf;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Arrays;
import java.util.Map;
import java.util.HashMap;
import java.util.Collections;
import java.util.Optional;

import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.defaults.internal.data.DefaultColumnarBatch;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.*;
import io.delta.kernel.expressions.*;
import io.delta.kernel.types.*;
import io.delta.kernel.defaults.engine.*;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.utils.CloseableIterable;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.DataFileStatus;
import io.delta.kernel.utils.FileStatus;
import io.delta.kernel.internal.InternalScanFileUtils;
import io.delta.kernel.internal.data.ScanStateRow;
import io.delta.kernel.engine.Engine;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;

import org.greenplum.pxf.api.model.BasePlugin;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.model.Accessor;
import java.util.logging.Level;
import java.util.logging.Logger;

import java.time.Duration;
import java.time.Instant;

import com.example.pxf.partitioning.DeltaVectorizedFragmentMetadata;
import com.example.pxf.DeltaUtilities;

import static io.delta.kernel.internal.util.Utils.singletonCloseableIterator;
import static io.delta.kernel.internal.util.Utils.toCloseableIterator;

/**
 * Accessor class for reading DeltaTable data in batches.
 * Each batch contains rows of all projected columns.
 */
public class DeltaTableVectorizedAccessor extends BasePlugin implements Accessor {

    private static final Logger LOG = Logger.getLogger(DeltaTableVectorizedAccessor.class.getName());
    private CloseableIterator<Row> rowIterator;
    private static final int DEFAULT_BATCH_SIZE = 1024;
    private static final String BATCH_SIZE_PROPERTY = "batch_size";
    private Engine engine;
    private Snapshot snapshot;
    private Scan scan;
    private CloseableIterator<FilteredColumnarBatch> scanFileIter;
    private CloseableIterator<FilteredColumnarBatch> transformedData;
    private CloseableIterator<Row> scanFileRows;
    private DeltaVectorizedFragmentMetadata fragmentMeta;
    private Row scanState ;
    private StructType tableSchema;
    private Predicate recordFilter;
    private TransactionBuilder txnBuilder;
    private List<FilteredColumnarBatch> filteredBatches = new ArrayList<>();
    private List<Row> dataActions = new ArrayList<>();
    private Transaction txn;
    private boolean isParentPath = false;
    int batchSize = 1024;

     @Override
    public void afterPropertiesSet() {
        try {
            fragmentMeta = context.getFragmentMetadata();
            Configuration hadoopConf = initializeHadoopConfiguration();
            engine = DefaultEngine.create(hadoopConf);
        } catch (Exception e) {
            LOG.log(Level.SEVERE, "Error initialize Delta table", e);
        }
    }

    @Override
    public boolean openForRead() throws Exception {
        String tablePath = context.getDataSource();
        String partitionInfo = "";
        isParentPath = DeltaUtilities.isParentPath(tablePath);
        if (isParentPath) {
            partitionInfo = fragmentMeta.getPartitionInfo();
            if (partitionInfo == null || partitionInfo.isEmpty()) {
                LOG.fine("Partition filter is empty");
            } else {
                tablePath = tablePath + "/" + partitionInfo;
                partitionInfo = "";
            }
        } else {
            partitionInfo = fragmentMeta.getPartitionInfo();
        }
        LOG.info("Opening DeltaLog for table path: " + tablePath);

        Table deltaTable = Table.forPath(engine, tablePath);
        snapshot = deltaTable.getLatestSnapshot(engine);
        ScanBuilder scanBuilder = snapshot.getScanBuilder(engine);
        StructType readSchema = DeltaUtilities.getReadSchema(context.getTupleDescription(), snapshot.getPartitionColumnNames(engine));

        // Extract partition filter from fragment metadata
        Predicate partitionPredicate = null;
        if (partitionInfo == null || partitionInfo.isEmpty()) {
            LOG.fine("Partition filter is empty");
        } else {
            String[] filterStrs = partitionInfo.split("=");
            if (filterStrs.length == 2) {
                partitionPredicate = DeltaUtilities.getPartitionPredicate(filterStrs[0], filterStrs[1], readSchema);
            } else {
                LOG.info("Invalid partition filter: " + partitionInfo);
            }
        }
        // handle column projection
        scanBuilder = scanBuilder.withReadSchema(engine, readSchema);

        recordFilter = DeltaUtilities.getFilterPredicate(context.getFilterString(), readSchema, context.getTupleDescription());
        // set filter as recordFilter if it is not null
        if (recordFilter != null) {
            LOG.info("Filter predicate: " + recordFilter);
            scanBuilder = scanBuilder.withFilter(engine, recordFilter);
        } else if (partitionPredicate != null) {
            LOG.info("Partition filter predicate: " + partitionPredicate);
            scanBuilder = scanBuilder.withFilter(engine, partitionPredicate);
        }

        scan = scanBuilder.build();
        scanState = scan.getScanState(engine);
        scanFileIter = scan.getScanFiles(engine);

        // Get batch size from context or use default
        String batchSizeParam = context.getOption("batch_size");
        batchSize = (batchSizeParam != null) ? Integer.parseInt(batchSizeParam) : DEFAULT_BATCH_SIZE;
            LOG.info("Batch size set to: " + batchSize);

        return true;
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
                LOG.fine("Processing file: " + filePath);
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
    public void closeForRead() throws Exception {
        if (rowIterator != null) {
            rowIterator.close();
        }
        LOG.info("DeltaTableVectorizedAccessor closed.");
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
                // Set the schema of the new table on the transaction builder
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
        LOG.info("table path: " + tablePath);
        createTransaction(tablePath);
        return true;
    }

    @Override
    public OneRow readNextObject() {
        try {
            final Instant start = Instant.now(); // Start time tracking
            List<Row> batch = new ArrayList<>();

            // Collect a batch of rows
            while (rowIterator != null && rowIterator.hasNext() && batch.size() < batchSize) {
                batch.add(rowIterator.next());
            }

            if (!batch.isEmpty()) {
                final long elapsedMillis = Duration.between(start, Instant.now()).toMillis();
                LOG.info(String.format("Read batch of %d rows in %d milliseconds", batch.size(), elapsedMillis));
                return new OneRow(null, batch); // Return the batch
            }

            // If no rows are left in the current file, move to the next file
            if (readNextFiltedData()) {
                return readNextObject(); // Recursive call to process the next file
            }
        } catch (Exception e) {
            LOG.log(Level.SEVERE, "Error reading next batch of objects", e);
        }
        return null; // No more data available
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

    private void verifyCommitSuccess(String tablePath, TransactionCommitResult result) {
        // Verify the commit was successful
        if (result.getVersion() >= 0) {
            System.out.println("Table created/commited successfully at: " + tablePath);
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
        ColumnarBatch batch = new DefaultColumnarBatch(columns[0].getSize(), tableSchema, columns);
        FilteredColumnarBatch filteredBatch = new FilteredColumnarBatch(batch, Optional.empty());
        filteredBatches.add(filteredBatch);
        if (filteredBatches.size() >= 10) {
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
}
