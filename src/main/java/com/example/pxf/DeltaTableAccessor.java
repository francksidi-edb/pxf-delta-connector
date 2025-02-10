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


public class DeltaTableAccessor extends BasePlugin implements org.greenplum.pxf.api.model.Accessor {

    private static final Logger LOG = Logger.getLogger(DeltaTableAccessor.class.getName());
    private DeltaLog deltaLog;
    private Snapshot snapshot;
    private StructType schema;
    private List<StructField> fields;
    private Iterator<String> fileIterator;
    private CloseableIterator<RowRecord> rowIterator;

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

    private boolean openNextFile() {
        closeCurrentRowIterator(); // Ensure any previously open iterator is closed
        if (fileIterator.hasNext()) {
            try {
                String filePath = fileIterator.next();
                LOG.info("Processing file: " + filePath);
                rowIterator = snapshot.open(); // Open the iterator for the whole Delta table
                return true;
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
                RowRecord row = rowIterator.next();
                return new OneRow(null, extractRowValues(row, fields));
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

