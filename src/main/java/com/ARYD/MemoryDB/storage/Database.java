package com.ARYD.MemoryDB.storage;

import com.ARYD.MemoryDB.index.Index;
import com.ARYD.MemoryDB.index.IndexFactory;
import com.ARYD.MemoryDB.types.DataTypeFactory;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@Component
public class Database {
    private final Map<String, Table> tables;
    private final Map<String, Index> indexes;
    private final AtomicInteger nextTableId;
    private final AtomicInteger nextIndexId;
    private final IndexFactory indexFactory;

    public Database(IndexFactory indexFactory) {
        this.tables = new ConcurrentHashMap<>();
        this.indexes = new ConcurrentHashMap<>();
        this.nextTableId = new AtomicInteger(1);
        this.nextIndexId = new AtomicInteger(1);
        this.indexFactory = indexFactory;
    }

    public Table createTable(String name, List<ColumnDefinition> columns) {
        Table table = new Table(name, columns);
        tables.put(name, table);
        return table;
    }

    public Table getTable(String name) {
        return tables.get(name);
    }

    public Index createIndex(String name, Table table, List<String> columns) {
        Index index = indexFactory.createIndex(table, columns);
        indexes.put(name, index);
        return index;
    }

    public Index getIndex(String name) {
        return indexes.get(name);
    }

    public void dropTable(String name) {
        Table table = tables.remove(name);
        if (table != null) {
            // Supprimer tous les index associés à cette table
            indexes.entrySet().removeIf(entry -> entry.getValue().getTable().equals(table));
        }
    }

    public void dropIndex(String name) {
        indexes.remove(name);
    }

    public List<String> getTableNames() {
        return new ArrayList<>(tables.keySet());
    }

    public List<String> getIndexNames() {
        return new ArrayList<>(indexes.keySet());
    }

    public int getNextTableId() {
        return nextTableId.getAndIncrement();
    }

    public int getNextIndexId() {
        return nextIndexId.getAndIncrement();
    }
} 
 