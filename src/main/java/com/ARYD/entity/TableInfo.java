package com.ARYD.entity;

import java.util.List;
import java.util.Map;
import java.time.LocalDateTime;

/**
 * DTO contenant les informations sur une table
 */
public class TableInfo {
    private String name;
    private int rowCount;
    private int columnCount;
    private List<String> columnNames;
    private long memoryUsage;
    private LocalDateTime creationTime;
    private Map<String, Object> indexes;

    public TableInfo() {
        // Constructeur par défaut requis pour JAX-RS
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getRowCount() {
        return rowCount;
    }

    public void setRowCount(int rowCount) {
        this.rowCount = rowCount;
    }

    public int getColumnCount() {
        return columnCount;
    }

    public void setColumnCount(int columnCount) {
        this.columnCount = columnCount;
    }

    public List<String> getColumnNames() {
        return columnNames;
    }

    public void setColumnNames(List<String> columnNames) {
        this.columnNames = columnNames;
    }

    public long getMemoryUsage() {
        return memoryUsage;
    }

    public void setMemoryUsage(long memoryUsage) {
        this.memoryUsage = memoryUsage;
    }

    public LocalDateTime getCreationTime() {
        return creationTime;
    }

    public void setCreationTime(LocalDateTime creationTime) {
        this.creationTime = creationTime;
    }

    public Map<String, Object> getIndexes() {
        return indexes;
    }

    public void setIndexes(Map<String, Object> indexes) {
        this.indexes = indexes;
    }
} 
 