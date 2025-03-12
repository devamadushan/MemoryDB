package com.ARYD.MemoryDB.repository;

import com.ARYD.MemoryDB.model.Table;
import org.springframework.stereotype.Repository;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Repository
public class TableDao {
    private final Map<String, Table> tables = new ConcurrentHashMap<>();

    public void saveTable(String name, Table table) {
        tables.put(name, table);
    }

    public Table getTable(String name) {
        return tables.get(name);
    }

    public boolean tableExists(String name) {
        return tables.containsKey(name);
    }

    public void deleteTable(String name) {
        tables.remove(name);
    }

    public Map<String, Table> getAllTables() {
        return tables;
    }
}
