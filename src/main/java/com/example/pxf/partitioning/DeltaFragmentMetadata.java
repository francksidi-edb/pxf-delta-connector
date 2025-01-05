package com.example.pxf.partitioning;
import org.greenplum.pxf.api.utilities.FragmentMetadata;
import java.io.Serializable;

public class DeltaFragmentMetadata implements FragmentMetadata, Serializable {

    private final String filePath;
    private final long startOffset;
    private final long length;
    private final String partitionInfo;

    /**
     * Constructor for DeltaFragmentMetadata.
     *
     * @param filePath      the file path for this fragment
     * @param startOffset   the start offset of the fragment
     * @param length        the length of the fragment
     * @param partitionInfo the partition information
     */
    public DeltaFragmentMetadata(String filePath, long startOffset, long length, String partitionInfo) {
        this.filePath = filePath;
        this.startOffset = startOffset;
        this.length = length;
        this.partitionInfo = partitionInfo;
    }

    public long getStartOffset() {
        return startOffset;
    }

    public long getLength() {
        return length;
    }

    public String getFilePath() {
        return filePath;
    }

    public String getPartitionInfo() {
        return partitionInfo;
    }
}


