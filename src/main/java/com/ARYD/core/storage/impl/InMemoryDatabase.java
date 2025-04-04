package com.ARYD.core.storage.impl;

import com.ARYD.core.storage.Database;
import com.ARYD.core.storage.Table;
import com.ARYD.core.storage.ColumnDefinition;
import com.ARYD.exception.ARYDException;

import javax.inject.Singleton;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

/**
 * Implémentation en mémoire de la base de données
 */
@Singleton
public class InMemoryDatabase implements Database {

    private static final Logger logger = Logger.getLogger(InMemoryDatabase.class.getName());
    
    private final String name;
    private final Map<String, Table> tables;
    
    public InMemoryDatabase(String name) {
        this.name = name;
        this.tables = new ConcurrentHashMap<>();
        logger.info("Base de données en mémoire créée: " + name);
    }
    
    public InMemoryDatabase() {
        this("default");
    }
    
    @Override
    public String getName() {
        return name;
    }
    
    @Override
    public Table createTable(String tableName, List<ColumnDefinition> columns) {
        if (tables.containsKey(tableName)) {
            throw new ARYDException("La table '" + tableName + "' existe déjà", "TABLE_EXISTS");
        }
        
        Table table = new InMemoryTable(tableName, columns);
        tables.put(tableName, table);
        
        logger.info("Table créée: " + tableName + " avec " + columns.size() + " colonnes");
        return table;
    }
    
    @Override
    public Table createTable(String tableName) {
        return createTable(tableName, new ArrayList<>());
    }
    
    @Override
    public Optional<Table> getTable(String tableName) {
        return Optional.ofNullable(tables.get(tableName));
    }
    
    @Override
    public boolean hasTable(String tableName) {
        return tables.containsKey(tableName);
    }
    
    @Override
    public boolean dropTable(String tableName) {
        Table table = tables.remove(tableName);
        if (table != null) {
            table.drop();
            logger.info("Table supprimée: " + tableName);
            return true;
        }
        return false;
    }
    
    @Override
    public List<String> getTableNames() {
        return new ArrayList<>(tables.keySet());
    }
    
    @Override
    public long getMemorySize() {
        long totalSize = 0;
        
        // Obtenir la taille approximative en mémoire de toutes les tables
        for (Table table : tables.values()) {
            Map<String, Object> stats = table.getStatistics();
            if (stats.containsKey("memorySize")) {
                totalSize += (long) stats.get("memorySize");
            }
        }
        
        return totalSize;
    }
    
    @Override
    public Object exportSchema() {
        Map<String, List<Map<String, Object>>> schema = new HashMap<>();
        
        for (String tableName : tables.keySet()) {
            Table table = tables.get(tableName);
            List<Map<String, Object>> columnsData = new ArrayList<>();
            
            for (ColumnDefinition column : table.getColumns()) {
                Map<String, Object> columnInfo = new HashMap<>();
                columnInfo.put("name", column.getName());
                columnInfo.put("type", column.getTypeName());
                columnInfo.put("nullable", column.isNullable());
                columnInfo.put("indexed", column.isIndexed());
                
                if (column.getDescription() != null) {
                    columnInfo.put("description", column.getDescription());
                }
                
                if (column.getFormat() != null) {
                    columnInfo.put("format", column.getFormat());
                }
                
                if (column.getDefaultValue() != null) {
                    columnInfo.put("defaultValue", column.getDefaultValue());
                }
                
                columnsData.add(columnInfo);
            }
            
            schema.put(tableName, columnsData);
        }
        
        return schema;
    }
    
    @Override
    public boolean importSchema(Object schema) {
        if (!(schema instanceof Map)) {
            throw new ARYDException("Format de schéma invalide", "INVALID_SCHEMA");
        }
        
        try {
            @SuppressWarnings("unchecked")
            Map<String, List<Map<String, Object>>> schemaMap = (Map<String, List<Map<String, Object>>>) schema;
            
            for (String tableName : schemaMap.keySet()) {
                List<Map<String, Object>> columnsData = schemaMap.get(tableName);
                List<ColumnDefinition> columns = new ArrayList<>();
                
                for (Map<String, Object> columnInfo : columnsData) {
                    String name = (String) columnInfo.get("name");
                    String type = (String) columnInfo.get("type");
                    boolean nullable = columnInfo.containsKey("nullable") ? (boolean) columnInfo.get("nullable") : false;
                    boolean indexed = columnInfo.containsKey("indexed") ? (boolean) columnInfo.get("indexed") : false;
                    
                    ColumnDefinition.Builder builder = ColumnDefinition.builder(name, type);
                    
                    if (nullable) {
                        builder.nullable();
                    }
                    
                    if (indexed) {
                        builder.indexed();
                    }
                    
                    if (columnInfo.containsKey("description")) {
                        builder.description((String) columnInfo.get("description"));
                    }
                    
                    if (columnInfo.containsKey("format")) {
                        builder.format((String) columnInfo.get("format"));
                    }
                    
                    if (columnInfo.containsKey("defaultValue")) {
                        builder.defaultValue(String.valueOf(columnInfo.get("defaultValue")));
                    }
                    
                    columns.add(builder.build());
                }
                
                // Si la table existe déjà, la supprimer d'abord
                if (hasTable(tableName)) {
                    dropTable(tableName);
                }
                
                createTable(tableName, columns);
            }
            
            logger.info("Schéma importé avec succès: " + schemaMap.size() + " tables");
            return true;
        } catch (Exception e) {
            throw new ARYDException("Erreur lors de l'importation du schéma: " + e.getMessage(), 
                    e, "SCHEMA_IMPORT_ERROR");
        }
    }
    
    @Override
    public void truncateAll() {
        for (Table table : tables.values()) {
            table.truncate();
        }
        logger.info("Toutes les tables ont été vidées");
    }
    
    @Override
    public void dropAll() {
        List<String> tableNames = new ArrayList<>(tables.keySet());
        
        for (String tableName : tableNames) {
            dropTable(tableName);
        }
        
        logger.info("Toutes les tables ont été supprimées");
    }
    
    /**
     * Récupérer des statistiques sur la base de données
     */
    public Map<String, Object> getStatistics() {
        Map<String, Object> stats = new HashMap<>();
        
        stats.put("name", name);
        stats.put("tableCount", tables.size());
        
        long totalRows = 0;
        for (Table table : tables.values()) {
            totalRows += table.getRowCount();
        }
        
        stats.put("totalRows", totalRows);
        stats.put("memorySize", getMemorySize());
        stats.put("tableNames", getTableNames());
        
        return stats;
    }
    
    @Override
    public String toString() {
        return "InMemoryDatabase[name=" + name + ", tables=" + tables.size() + "]";
    }
} 
 