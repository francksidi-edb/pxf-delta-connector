package com.example.pxf;

import io.delta.standalone.DeltaLog;
import io.delta.standalone.Snapshot;
import io.delta.standalone.types.StructType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.greenplum.pxf.api.model.BaseFragmenter;
import org.greenplum.pxf.api.model.Fragment;
import com.example.pxf.partitioning.DeltaFragmentMetadata;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DeltaTableFragmenter extends BaseFragmenter {

    private static final Logger LOG = Logger.getLogger(DeltaTableFragmenter.class.getName());
    private DeltaLog deltaLog;
    private StructType schema;

    @Override
    public void afterPropertiesSet() {
        try {
            String deltaTablePath = context.getDataSource();
            LOG.info("Initializing DeltaLog for table path: " + deltaTablePath);

            Configuration hadoopConf = initializeHadoopConfiguration();
            deltaLog = DeltaLog.forTable(hadoopConf, new Path(deltaTablePath));
            Snapshot snapshot = deltaLog.snapshot();
            schema = snapshot.getMetadata().getSchema();

            LOG.info("Schema initialized: " + schema);
        } catch (Exception e) {
            LOG.log(Level.SEVERE, "Failed to initialize DeltaLog", e);
            throw new RuntimeException("Failed to initialize DeltaLog", e);
        }
    }

    @Override
    public List<Fragment> getFragments() {
        List<Fragment> fragments = new ArrayList<>();
        try {
            Snapshot snapshot = deltaLog.snapshot();

            // Check if the table has partitions
            if (!snapshot.getMetadata().getPartitionColumns().isEmpty()) {
                LOG.info("Fragmenting based on partitions...");
                List<String> partitionColumns = snapshot.getMetadata().getPartitionColumns();
                snapshot.getAllFiles().forEach(file -> {
                    String partitionInfo = extractPartitionInfo(file.getPath(), partitionColumns);
                    DeltaFragmentMetadata metadata = new DeltaFragmentMetadata(file.getPath(), partitionInfo);
                    fragments.add(new Fragment(context.getDataSource(), metadata, null));
                });
            } else {
                LOG.info("Fragmenting based on file splits...");
                snapshot.getAllFiles().forEach(file -> {
                    DeltaFragmentMetadata metadata = new DeltaFragmentMetadata(file.getPath(), null);
                    fragments.add(new Fragment(context.getDataSource(), metadata, null));
                });
            }
        } catch (Exception e) {
            LOG.log(Level.SEVERE, "Error during fragmentation", e);
            throw new RuntimeException("Error during fragmentation", e);
        }
        return fragments;
    }

    private Configuration initializeHadoopConfiguration() {
        Configuration hadoopConf = new Configuration();
        hadoopConf.set("fs.defaultFS", "file:///");
        hadoopConf.set("mapreduce.framework.name", "local");
        hadoopConf.set("hadoop.tmp.dir", "/tmp/hadoop");
        LOG.info("Hadoop configuration initialized.");
        return hadoopConf;
    }

    private String extractPartitionInfo(String filePath, List<String> partitionColumns) {
        // Example implementation: Extract partition information from file path
        // Delta table encodes partition values in the file path
        StringBuilder partitionInfo = new StringBuilder();
        for (String partition : partitionColumns) {
            // Example: filePath contains "year=2021/month=12/"
            if (filePath.contains(partition + "=")) {
                int startIndex = filePath.indexOf(partition + "=") + partition.length() + 1;
                int endIndex = filePath.indexOf("/", startIndex);
                String value = endIndex == -1 ? filePath.substring(startIndex) : filePath.substring(startIndex, endIndex);
                partitionInfo.append(partition).append("=").append(value).append(",");
            }
        }
        if (partitionInfo.length() > 0) {
            partitionInfo.setLength(partitionInfo.length() - 1); // Remove trailing comma
        }
        return partitionInfo.toString();
    }
}


