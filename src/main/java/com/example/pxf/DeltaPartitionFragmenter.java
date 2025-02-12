package com.example.pxf;

import org.greenplum.pxf.api.model.BaseFragmenter;
import org.greenplum.pxf.api.model.Fragment;
import org.greenplum.pxf.api.model.FragmentStats;
import com.example.pxf.partitioning.DeltaVectorizedFragmentMetadata;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;
import com.fasterxml.jackson.databind.ObjectMapper;
/**
 * Delta Table Fragmenter
 * Handles partitioned Delta Tables with folders named by year (e.g., 2014, 2023).
 */
public class DeltaPartitionFragmenter extends BaseFragmenter {

    private String basePath;
    private static final Logger LOG = Logger.getLogger(DeltaPartitionFragmenter.class.getName());
      private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();


    @Override
    public void afterPropertiesSet() {
        // Fetch the base path for the Delta Table
        basePath = context.getDataSource();
        if (basePath == null || basePath.isEmpty()) {
            throw new IllegalArgumentException("DATA_PATH option is required");
        }

        File baseDir = new File(basePath);
        if (!baseDir.exists() || !baseDir.isDirectory()) {
            throw new IllegalArgumentException("The specified DATA_PATH is not a valid directory: " + basePath);
        }

        LOG.info("Initialized DeltaPartitionFragmenter with base path: " + basePath);
    }
@Override
public List<Fragment> getFragments() {
    List<Fragment> fragments = new ArrayList<>();

    // Iterate through partition folders (e.g., 2014, 2023)
    File baseDir = new File(basePath);
    File[] partitions = baseDir.listFiles(File::isDirectory);

    if (partitions != null) {
        for (File partition : partitions) {
            String partitionName = partition.getName(); // e.g., "2023"

            LOG.info("Processing partition: " + partitionName);

            // Create a fragment for the partition root
            String partitionPath = partition.getPath();
            DeltaVectorizedFragmentMetadata metadata = new DeltaVectorizedFragmentMetadata(partitionPath, partitionName);

            Fragment fragment = new Fragment(partitionPath, metadata, null);

            LOG.info("Adding fragment for partition: " + partitionPath);

            fragments.add(fragment);
        }
    } else {
        LOG.warning("No partitions found in base path: " + basePath);
    }

    LOG.info("Total fragments created: " + fragments.size());
    return fragments;
}

    @Override
    public FragmentStats getFragmentStats() throws UnsupportedOperationException {
        throw new UnsupportedOperationException("ANALYZE for Delta Partition Plugin is not supported");
    }

}
