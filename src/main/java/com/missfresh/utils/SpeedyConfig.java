package com.missfresh.utils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SpeedyConfig  implements Serializable {
    private String jobName;
    private String snapshotPath;
    private String snapshotFile;
    private String colFamily;
    private String targetTable;
    private String targetBasePath;
    private List<String> partitionBy = new ArrayList<String>();
    private Map<String, String> objectVO = new HashMap<String, String>();

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public String getSnapshotPath() {
        return snapshotPath;
    }

    public void setSnapshotPath(String snapshotPath) {
        this.snapshotPath = snapshotPath;
    }

    public String getSnapshotFile() {
        return snapshotFile;
    }

    public void setSnapshotFile(String snapshotFile) {
        this.snapshotFile = snapshotFile;
    }

    public String getColFamily() {
        return colFamily;
    }

    public void setColFamily(String colFamily) {
        this.colFamily = colFamily;
    }

    public String getTargetTable() {
        return targetTable;
    }

    public void setTargetTable(String targetTable) {
        this.targetTable = targetTable;
    }

    public String getTargetBasePath() {
        return targetBasePath;
    }

    public void setTargetBasePath(String targetBasePath) {
        this.targetBasePath = targetBasePath;
    }

    public List<String> getPartitionBy() {
        return partitionBy;
    }

    public void setPartitionBy(List<String> partitionBy) {
        this.partitionBy = partitionBy;
    }

    public Map<String, String> getObjectVO() {
        return objectVO;
    }

    public void setObjectVO(Map<String, String> objectVO) {
        this.objectVO = objectVO;
    }
}
