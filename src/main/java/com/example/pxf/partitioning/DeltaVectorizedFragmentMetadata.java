package com.example.pxf.partitioning;

import org.greenplum.pxf.api.utilities.FragmentMetadata;

import java.io.Serializable;

/**
 * Metadata class for Delta Table fragments.
 */
public class DeltaVectorizedFragmentMetadata implements FragmentMetadata, Serializable {
    private final String filePath;
    private final String partitionInfo;

    public DeltaVectorizedFragmentMetadata(String filePath, String partitionInfo) {
        this.filePath = filePath;
        this.partitionInfo = partitionInfo;
    }

    public String getFilePath() {
        return filePath;
    }

    public String getPartitionInfo() {
        return partitionInfo;
    }

    /**
     * Converts partition metadata into a SQL constraint.
     *
     * @param quoteString the string to quote partition column names
     * @return a SQL-compatible WHERE clause (e.g., col='value')
     */
    public String toSqlConstraint(String quoteString) {
        if (partitionInfo == null || partitionInfo.isEmpty()) {
            return "";
        }
        // Example: partitionInfo is "year=2021,month=12"
        String[] parts = partitionInfo.split(",");
        StringBuilder sqlConstraint = new StringBuilder();
        for (String part : parts) {
            String[] kv = part.split("=");
            if (kv.length == 2) {
                sqlConstraint.append(quoteString).append(kv[0]).append(quoteString)
                        .append("=").append("'").append(kv[1]).append("' AND ");
            }
        }
        if (sqlConstraint.length() > 0) {
            sqlConstraint.setLength(sqlConstraint.length() - 5); // Remove trailing " AND "
        }
        return sqlConstraint.toString();
    }
}


