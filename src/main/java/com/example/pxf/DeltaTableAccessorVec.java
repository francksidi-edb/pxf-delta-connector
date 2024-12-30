package com.example.pxf;

import io.delta.standalone.DeltaLog;
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



    @Override
    public void afterPropertiesSet() {
        // No additional initialization required
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

	    // Get batch size from context or use default
	    String batchSizeParam = context.getOption("batch_size");
    batchSize = (batchSizeParam != null) ? Integer.parseInt(batchSizeParam) : DEFAULT_BATCH_SIZE;
        LOG.info("Batch size set to: " + batchSize);
	// Calculate and log the total rows in the Delta table

            // Collect all add file paths into an iterator
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
/*
    private boolean openNextFile() {
        closeCurrentRowIterator(); // Ensure any previously open iterator is closed
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
        return false; // No more files to process
    }


    */


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

/*
    @Override
    public OneRow readNextObject() {
        try {
            if (rowIterator != null && rowIterator.hasNext()) {
                RowRecord row = rowIterator.next();
                return new OneRow(null, extractRowValues(row, fields));
            } else if (openNextFile()) {
                return readNextObject(); // Recursively process the next file
            }
        } catch (Exception e) {
            LOG.log(Level.SEVERE, "Error reading next object", e);
        }
        return null;
    }
*/


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
            final long elapsedNanos = Duration.between(start, Instant.now()).toNanos();
            LOG.info(String.format("Read batch of %d rows in %d nanoseconds", batch.size(), elapsedNanos));
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

