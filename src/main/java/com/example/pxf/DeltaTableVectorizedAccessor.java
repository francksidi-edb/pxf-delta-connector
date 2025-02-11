package com.example.pxf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Arrays;
import java.util.Optional;

import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.*;
import io.delta.kernel.expressions.*;
import io.delta.kernel.types.*;
import io.delta.kernel.defaults.engine.*;

import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import io.delta.kernel.internal.InternalScanFileUtils;
import io.delta.kernel.internal.data.ScanStateRow;
import io.delta.kernel.engine.Engine;

import org.apache.hadoop.conf.Configuration;
import org.greenplum.pxf.api.model.BasePlugin;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.model.Accessor;
import java.util.logging.Level;
import java.util.logging.Logger;

import java.time.Duration;
import java.time.Instant;

import com.example.pxf.partitioning.DeltaVectorizedFragmentMetadata;

import static io.delta.kernel.internal.util.Utils.singletonCloseableIterator;

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
    LOG.info("Opening DeltaLog for table path: " + tablePath);

    Table deltaTable = Table.forPath(engine, context.getDataSource());
    snapshot = deltaTable.getLatestSnapshot(engine);
    ScanBuilder scanBuilder = snapshot.getScanBuilder(engine);

    // Extract partition filter from fragment metadata
    String partitionInfo = fragmentMeta.getPartitionInfo();
    if (partitionInfo == null || partitionInfo.isEmpty()) {
        LOG.fine("Partition filter is empty");
    } else {
        String[] filterStrs = partitionInfo.split("=");
        if (filterStrs.length == 2) {
            Predicate filter = new Predicate(
                "=",
                Arrays.asList(new Column(filterStrs[0]), Literal.ofString(filterStrs[1])));
            scanBuilder = scanBuilder.withFilter(engine, filter);
        } else {
            LOG.info("Invalid partition filter: " + partitionInfo);
        }
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
    public void closeForRead() throws Exception {
        if (rowIterator != null) {
            rowIterator.close();
        }
        LOG.info("DeltaTableVectorizedAccessor closed.");
    }
       @Override
    public boolean openForWrite() {
        throw new UnsupportedOperationException("Write operations are not supported by DeltaTableAccessor");
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
}
