package com.ARYD.MemoryDB.model;

import lombok.Getter;

import java.util.*;

public class Table {

    @Getter
    private final String name;
    @Getter
    private final List<String> columns; // Pour stocker les list des colonnes
    private final List<Map<String, Object>> rows; // Pour stocker les données
    private int rowCounter = 0;  // ID


    public Table(String name , List<String> columns) {
        this.name = name;
        this.columns = new ArrayList<>(columns);
        this.rows = new ArrayList<>();
    }


    public void addRow(Map<String, Object> row) {
        row.put("_rowId", rowCounter++);
        rows.add(row);
    }
    public List<Map<String, Object>> getAllRows() {
        return rows;
    }

    public List<Map<String, Object>> filterByColumn(String column, Object value) {
        List<Map<String, Object>> result = new ArrayList<>();
        for (Map<String, Object> row : rows) {
            if (value.equals(row.get(column))) {
                result.add(row);
            }
        }
        return result;
    }

    public List<Map<String, Object>> orderBy(String column, boolean ascending) {
        List<Map<String, Object>> sortedRows = new ArrayList<>(rows);
        sortedRows.sort(Comparator.comparing(row -> (Comparable) row.get(column)));
        if (!ascending) {
            Collections.reverse(sortedRows);
        }
        return sortedRows;
    }

    public Map<Object, List<Map<String, Object>>> groupBy(String column) {
        Map<Object, List<Map<String, Object>>> groupedData = new HashMap<>();
        for (Map<String, Object> row : rows) {
            Object key = row.get(column);
            groupedData.putIfAbsent(key, new ArrayList<>());
            groupedData.get(key).add(row);
        }
        return groupedData;
    }


}
