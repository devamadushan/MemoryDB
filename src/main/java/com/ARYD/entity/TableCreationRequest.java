package com.ARYD.entity;

import com.ARYD.MemoryDB.storage.ColumnDefinition;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * Request for creating a new table structure
 */
@Data
@NoArgsConstructor
public class TableCreationRequest {
    private String tableName;
    private List<ColumnDefinition> columns;
    private boolean distributed = true;
} 