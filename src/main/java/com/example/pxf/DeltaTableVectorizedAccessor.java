package com.example.pxf;

import org.apache.hadoop.io.LongWritable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import io.delta.standalone.DeltaLog;
import io.delta.standalone.Snapshot;
import io.delta.standalone.data.CloseableIterator;
import io.delta.standalone.data.RowRecord;
import io.delta.standalone.actions.AddFile;
import org.apache.hadoop.conf.Configuration;
import org.greenplum.pxf.api.model.BasePlugin;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.model.Accessor;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.Random;
import io.delta.standalone.types.StructField;
import io.delta.standalone.types.StructType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.time.Duration;
import java.time.Instant;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.stream.IntStream;
import com.example.pxf.partitioning.DeltaVectorizedFragmentMetadata;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;


/**
 * Accessor class for reading DeltaTable data in batches.
 * Each batch contains rows of all projected columns.
 */
public class DeltaTableVectorizedAccessor extends BasePlugin implements Accessor {

    private static final Logger LOG = Logger.getLogger(DeltaTableVectorizedAccessor.class.getName());
    private CloseableIterator<RowRecord> rowIterator;
    private Iterator<String> fileIterator;
    private List<RowRecord> currentBatch;
    private int batchIndex;
    private static final int DEFAULT_BATCH_SIZE = 1024;
    private static final String BATCH_SIZE_PROPERTY = "batch_size";
    private DeltaLog deltaLog;
    private Snapshot snapshot;
    private StructType schema;
    private List<StructField> fields;
    private long rowsRead;
    int batchSize = 1024;

     @Override
    public void afterPropertiesSet() {
        // No additional initialization required
    }



    @Override
public boolean openForRead() throws Exception {
    String fullPath = context.getDataSource(); // Full path like "/mnt/data/parquet/users_partition/year=2018"
    LOG.info("Opening DeltaLog for full path: " + fullPath);

    // Extract base table path and partition filter
    String tablePath = extractBaseTablePath(fullPath);
    String partitionFilter = extractPartitionFilter(fullPath);
    LOG.info("Extracted table path: " + tablePath);
    LOG.info("Extracted partition filter: " + partitionFilter);

    // Initialize Hadoop configuration
    Configuration hadoopConf = initializeHadoopConfiguration();
    deltaLog = DeltaLog.forTable(hadoopConf, new Path(tablePath));

    if (deltaLog == null) {
        LOG.severe("Failed to initialize DeltaLog for table path: " + tablePath);
        throw new Exception("DeltaLog initialization failed");
    }

    snapshot = deltaLog.snapshot();
    if (snapshot == null || snapshot.getAllFiles().isEmpty()) {
        LOG.warning("Snapshot is null or contains no files. Table might be empty or inaccessible.");
        return false;
    }

        // Get batch size from context or use default
	    String batchSizeParam = context.getOption("batch_size");
    batchSize = (batchSizeParam != null) ? Integer.parseInt(batchSizeParam) : DEFAULT_BATCH_SIZE;
        LOG.info("Batch size set to: " + batchSize);
	// Calculate and log the total rows in the Delta table

    LOG.info("Snapshot version: " + snapshot.getVersion());
    LOG.info("Snapshot schema: " + snapshot.getMetadata().getSchema().toString());

    // Filter files based on the partition filter
    fileIterator = snapshot.getAllFiles().stream()
            .filter(file -> file.getPath().contains(partitionFilter))
            .map(file -> file.getPath())
            .collect(Collectors.toList())
            .iterator();

    if (!fileIterator.hasNext()) {
        LOG.warning("No matching files found for partition: " + partitionFilter);
        return false;
    }

    LOG.info("Files to read: " + fileIterator);
    return true;
}

/**
 * Extracts the base table path from the full path.
 * Example: /mnt/data/parquet/users_partition/year=2018 -> /mnt/data/parquet/users_partition
 *
 * @param fullPath Full path including partition filter
 * @return Base table path
 */
private String extractBaseTablePath(String fullPath) {
    int lastSeparatorIndex = fullPath.lastIndexOf("/");
    return lastSeparatorIndex > 0 ? fullPath.substring(0, lastSeparatorIndex) : fullPath;
}

/**
 * Extracts the partition filter from the full path.
 * Example: /mnt/data/parquet/users_partition/year=2018 -> year=2018
 *
 * @param fullPath Full path including partition filter
 * @return Partition filter
 */
private String extractPartitionFilter(String fullPath) {
    int lastSeparatorIndex = fullPath.lastIndexOf("/");
    return lastSeparatorIndex > 0 ? fullPath.substring(lastSeparatorIndex + 1) : "";
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
        List<RowRecord> batch = new ArrayList<>();

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
        if (openNextFile()) {
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
