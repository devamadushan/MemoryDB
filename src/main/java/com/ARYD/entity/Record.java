package com.ARYD.entity;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * Data record for inter-node communication
 */
@Data
@NoArgsConstructor
public class Record {
    private String tableName;
    private Map<String, Object> data;
    private String loadingId;
} 