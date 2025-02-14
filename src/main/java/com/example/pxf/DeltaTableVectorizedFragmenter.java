package com.example.pxf;

import io.delta.standalone.DeltaLog;
import io.delta.standalone.Snapshot;
import io.delta.standalone.types.StructType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.greenplum.pxf.api.model.BaseFragmenter;
import org.greenplum.pxf.api.model.Fragment;
import com.example.pxf.partitioning.DeltaVectorizedFragmentMetadata;
import com.example.pxf.DeltaUtilities;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DeltaTableVectorizedFragmenter extends BaseFragmenter {

    private static final Logger LOG = Logger.getLogger(DeltaTableFragmenter.class.getName());
    private DeltaLog deltaLog;
    private boolean isParentPath = false;

    @Override
    public void afterPropertiesSet() {
        try {
            String deltaTablePath = context.getDataSource();
            LOG.info("Initializing DeltaLog for table path: " + deltaTablePath);
            isParentPath = DeltaUtilities.isParentPath(deltaTablePath);
        } catch (Exception e) {
            LOG.log(Level.SEVERE, "Failed to initialize DeltaLog", e);
            throw new RuntimeException("Failed to initialize DeltaLog", e);
        }
    }

    @Override
    public List<Fragment> getFragments() {
        List<Fragment> fragments = new ArrayList<>();
        try {
            if (isParentPath) {
                // Fragment based on the parent path
                File[] files = new File(context.getDataSource()).listFiles();
                if (files != null) {
                    for (File file : files) {
                        if (file.isDirectory() && DeltaUtilities.isDeltaTable(file.getPath())) {
                            String partitionInfo = file.getName();
                            DeltaVectorizedFragmentMetadata metadata = new DeltaVectorizedFragmentMetadata(file.getPath(), partitionInfo);
                            Fragment fragment = new Fragment(context.getDataSource(), metadata, null);
                            // For multiple nodes, assign segment ID for fragments.
                            if (context.getTotalSegments() > files.length) {
                                int segmentId = getSegmentIdFromPath(file.getName());
                                fragment.setSegmentId(segmentId);
                                LOG.info("Assign fragment for: " + file.getPath() + " with segment id: " + segmentId);
                            }
                            fragments.add(fragment);
                            LOG.info("Adding fragment for: " + file.getPath());
                        } else {
                            //throw exception
                            throw new RuntimeException("Invalid Delta Table path: " + file.getPath());
                        }
                    }
                }
            } else {
                // Fragment based on the table path
                String deltaTablePath = context.getDataSource();
                LOG.info("Initializing DeltaLog for table path: " + deltaTablePath);
                Configuration hadoopConf = initializeHadoopConfiguration();
                deltaLog = DeltaLog.forTable(hadoopConf, new Path(deltaTablePath));
                Snapshot snapshot = deltaLog.snapshot();

                // Check if the table has partitions
                if (!snapshot.getMetadata().getPartitionColumns().isEmpty()) {
                    LOG.info("Fragmenting based on partitions...");
                    List<String> partitionColumns = snapshot.getMetadata().getPartitionColumns();
                    snapshot.getAllFiles().forEach(file -> {
                        String partitionInfo = extractPartitionInfo(file.getPath(), partitionColumns);
                        DeltaVectorizedFragmentMetadata metadata = new DeltaVectorizedFragmentMetadata(file.getPath(), partitionInfo);
                        fragments.add(new Fragment(context.getDataSource(), metadata, null));
                    });
                } else {
                    LOG.info("Fragmenting based on file splits...");
                    snapshot.getAllFiles().forEach(file -> {
                        DeltaVectorizedFragmentMetadata metadata = new DeltaVectorizedFragmentMetadata(file.getPath(), null);
                        fragments.add(new Fragment(context.getDataSource(), metadata, null));
                    });
                }
        }
        } catch (Exception e) {
            LOG.log(Level.SEVERE, "Error during fragmentation", e);
            throw new RuntimeException("Error during fragmentation", e);
        }
        LOG.info("Total fragments created: " + fragments.size());
        return fragments;
    }

    private int getSegmentIdFromPath(String path) {
        // path name format is seg_<segment_id>
        // remove prefix "seg_" and return segment_id
        String segmentId = path.substring(4);
        return Integer.parseInt(segmentId);
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

