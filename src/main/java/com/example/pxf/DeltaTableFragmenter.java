package com.example.pxf;
import io.delta.standalone.DeltaLog;
import io.delta.standalone.Snapshot;
import io.delta.standalone.actions.AddFile;
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

            // Get total segments from PXF context or default to 8
            int totalSegments = context.getTotalSegments();
            if (totalSegments == 0) {
                LOG.warning("Total segments not found. Defaulting to 8.");
                totalSegments = 8;
            }

	    LOG.info("Starting fragment assignment.");
            DeltaFragmentMetadata metadata = new DeltaFragmentMetadata(context.getDataSource(), 0, totalSegments, null);
            fragments.add(new Fragment(context.getDataSource(), metadata, null));
	    LOG.info("Completed fragment assignment.");
            LOG.info("Total fragments created: " + fragments.size());

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
}

