package com.ARYD.entity;

import java.util.List;
import java.util.Map;

/**
 * Représente le résultat d'une requête sur les données
 */
public class QueryResult {
    private String tableName;
    private int totalRows;
    private int returnedRows;
    private int offset;
    private List<String> columns;
    private List<Map<String, Object>> rows;
    private Map<String, Object> statistics;
    private long executionTimeMs;

    public QueryResult() {
        // Constructeur par défaut requis pour JAX-RS
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public int getTotalRows() {
        return totalRows;
    }

    public void setTotalRows(int totalRows) {
        this.totalRows = totalRows;
    }

    public int getReturnedRows() {
        return returnedRows;
    }

    public void setReturnedRows(int returnedRows) {
        this.returnedRows = returnedRows;
    }

    public int getOffset() {
        return offset;
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }

    public List<String> getColumns() {
        return columns;
    }

    public void setColumns(List<String> columns) {
        this.columns = columns;
    }

    public List<Map<String, Object>> getRows() {
        return rows;
    }

    public void setRows(List<Map<String, Object>> rows) {
        this.rows = rows;
    }

    public Map<String, Object> getStatistics() {
        return statistics;
    }

    public void setStatistics(Map<String, Object> statistics) {
        this.statistics = statistics;
    }

    public long getExecutionTimeMs() {
        return executionTimeMs;
    }

    public void setExecutionTimeMs(long executionTimeMs) {
        this.executionTimeMs = executionTimeMs;
    }
} 
 