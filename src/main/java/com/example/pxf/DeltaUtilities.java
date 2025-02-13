package com.example.pxf;

import java.io.File;
import java.util.logging.Logger;

public class DeltaUtilities {
    private static final Logger LOG = Logger.getLogger(DeltaTableFragmenter.class.getName());

    public static boolean isDeltaTable(String tablePath) {
        // check the tablePath/_delta_log directory exists
        return new File(tablePath + "/_delta_log").exists();
    }

    public static boolean isParentPath(String path) {
        // check if the path is a parent path of a delta table
        if (isDeltaTable(path)) {
            return false;
        }
        // check if the path contains a delta table 
        File[] files = new File(path).listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isDirectory() && isDeltaTable(file.getPath())) {
                    continue;
                } else {
                    // should error out here
                    return false;
                }
            }
        }
        return true;
    }
}
