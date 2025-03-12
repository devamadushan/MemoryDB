package com.ARYD.MemoryDB.service;

import com.ARYD.MemoryDB.entity.Column;
import com.ARYD.MemoryDB.entity.Table;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import java.util.stream.Collectors;
import java.util.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
@RequiredArgsConstructor
@Slf4j
public class TableService {
    final Map<String, Table> tables = new HashMap<>();

    public Table createTable(String name, List<Column> columns) throws IllegalArgumentException {
        if (tables.containsKey(name)) {
            throw new IllegalArgumentException("Table " + name + " already exists");
        }
        Table table = new Table(name, columns, new ArrayList<>());
        tables.put(name, table);
        return table;
    }

    public void deleteTable(String name) throws IllegalArgumentException {
        if (!tables.containsKey(name)) {
            throw new IllegalArgumentException("Table " + name + " does not exist");
        }
        tables.remove(name);
    }

    public Table insertRow(String name, Map<String, Object> row) throws IllegalArgumentException {
        if (!tables.containsKey(name)) {
            throw new IllegalArgumentException("Table " + name + " does not exist");
        }
        Table table = tables.get(name);
        table.getRows().add(row);
        return table;
    }

    public Table updateTable(String name, List<Column> columns) throws IllegalArgumentException {
        if (!tables.containsKey(name)) {
            throw new IllegalArgumentException("Table " + name + " does not exist");
        }
        Table table = tables.get(name);
        table.setColumns(columns);
        return table;
    }

    public List<Table> getTables() {
        return new ArrayList<>(tables.values());
    }


    public List<Object> getColumnValues(String tableName, String columnName) {
        Table table = tables.get(tableName);
        if (table == null) {
            return new ArrayList<>();
        }
        // Créer une copie de la liste des lignes pour éviter les modifications concurrentes
        List<Map<String, Object>> rowsCopy = new ArrayList<>(table.getRows());
        List<Object> values = new ArrayList<>();
        for (Map<String, Object> row : rowsCopy) {
            values.add(row.get(columnName));
        }
        return values;
    }






}
