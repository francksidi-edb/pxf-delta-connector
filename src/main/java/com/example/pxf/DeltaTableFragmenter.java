package com.example.pxf;
import io.delta.standalone.DeltaLog;
import io.delta.standalone.Snapshot;
import io.delta.standalone.actions.AddFile;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.greenplum.pxf.api.model.BaseFragmenter;
import org.greenplum.pxf.api.model.Fragment;
import com.example.pxf.partitioning.DeltaFragmentMetadata;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DeltaTableFragmenter extends BaseFragmenter {

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
            // Get total segments from PXF context or default to 8
            int totalSegments = context.getTotalSegments();
            if (totalSegments == 0) {
                LOG.warning("Total segments not found. Defaulting to 8.");
                totalSegments = 8;
            }

            if (isParentPath) {
                // Fragment based on the parent path
                File[] files = new File(context.getDataSource()).listFiles();
                if (files != null) {
                    int i = 0;
                    for (File file : files) {
                        if (file.isDirectory() && DeltaUtilities.isDeltaTable(file.getPath())) {
                            String partitionInfo = file.getName();
                            DeltaFragmentMetadata metadata = new DeltaFragmentMetadata(file.getPath(), i++ % totalSegments, totalSegments, partitionInfo);
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
                Configuration hadoopConf = initializeHadoopConfiguration();
                deltaLog = DeltaLog.forTable(hadoopConf, new Path(context.getDataSource()));
                Snapshot snapshot = deltaLog.snapshot();

                List<AddFile> allFiles = snapshot.getAllFiles();
                int totalFiles = allFiles.size();
                LOG.info("Total files available: " + totalFiles);
                LOG.info("Total segments configured: " + totalSegments);
                for (int i = 0; i < totalFiles; i++) {
                    AddFile file = allFiles.get(i);
                    String filePath = file.getPath();
                    int assignedSegment = i % totalSegments;
                    LOG.info("Assigning file: " + filePath + " to segment: " + assignedSegment);
                    DeltaFragmentMetadata metadata = new DeltaFragmentMetadata(filePath, assignedSegment, totalSegments, null);
                    fragments.add(new Fragment(context.getDataSource(), metadata, null));
                }

                LOG.info("Total fragments created: " + fragments.size());
            }
        } catch (Exception e) {
            LOG.log(Level.SEVERE, "Error during fragmentation", e);
            throw new RuntimeException("Error during fragmentation", e);
        }
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
}

