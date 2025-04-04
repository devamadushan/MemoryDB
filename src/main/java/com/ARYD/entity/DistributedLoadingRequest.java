package com.ARYD.entity;

import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Request for loading data into a distributed table
 */
@Data
@NoArgsConstructor
public class DistributedLoadingRequest {
    private String tableName;
    private String filePath;
    private int maxRows = -1;
    private String distributionStrategy = "ROUND_ROBIN"; // ROUND_ROBIN or HASH
    private String distributionColumn; // Only used for HASH strategy
} 