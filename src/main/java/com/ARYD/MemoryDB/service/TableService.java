package com.ARYD.MemoryDB.service;

import com.ARYD.MemoryDB.entity.Column;
import com.ARYD.MemoryDB.entity.Table;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
@RequiredArgsConstructor
@Slf4j
public class
TableService {
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

}
