package com.ARYD.MemoryDB.service;

import com.ARYD.MemoryDB.entity.DataFrame;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

@Service
public class TableService {

    // Stockage simple en mémoire des DataFrames par nom de table
    private Map<String, DataFrame> tables = new HashMap<>();

    public DataFrame getTableByName(String tableName) {
        return tables.get(tableName);
    }

    public void addTable(DataFrame df) {
        tables.put(df.getTableName(), df);
    }
    
    /**
     * Returns all table names in the system
     * @return a set of all table names
     */
    public Set<String> getAllTableNames() {
        return tables.keySet();
    }
}