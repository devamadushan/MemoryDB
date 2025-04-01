package com.ARYD.MemoryDB.service;

import com.ARYD.MemoryDB.entity.DataFrame;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

@Service
public class DataFrameService {

    // Stockage simple en mémoire des DataFrames par nom de table
    private Map<String, DataFrame> tables = new HashMap<>();

    public DataFrame getTableByName(String tableName) {
        return tables.get(tableName);
    }

    public void addTable(DataFrame df) {
        tables.put(df.getTableName(), df);
    }
}