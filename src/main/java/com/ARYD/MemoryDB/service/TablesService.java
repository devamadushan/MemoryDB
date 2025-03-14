package com.ARYD.MemoryDB.service;

import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

@Service
public class TablesService {

    // Stockage simple en mémoire des DataFrames par nom de table
    private Map<String, DataFrameService> tables = new HashMap<>();

    public DataFrameService getTableByName(String tableName) {
        return tables.get(tableName);
    }

    public void addTable(DataFrameService df) {
        tables.put(df.getTableName(), df);
    }
}