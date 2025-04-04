package com.ARYD.core.query;

import com.ARYD.core.storage.Database;
import com.ARYD.core.storage.Query;
import com.ARYD.core.storage.Table;
import com.ARYD.entity.QueryResult;
import com.ARYD.exception.ARYDException;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Moteur de requêtes qui permet d'exécuter des requêtes sur les tables
 */
@Singleton
public class QueryEngine {
    
    private static final Logger logger = Logger.getLogger(QueryEngine.class.getName());
    
    private final Database database;
    private final Map<String, Table> registeredTables;
    
    @Inject
    public QueryEngine(Database database) {
        this.database = database;
        this.registeredTables = new HashMap<>();
        
        // Précharger les tables existantes
        for (String tableName : database.getTableNames()) {
            database.getTable(tableName).ifPresent(table -> 
                registeredTables.put(tableName, table)
            );
        }
    }
    
    /**
     * Enregistre une table dans le moteur de requêtes
     */
    public void registerTable(Table table) {
        if (table != null) {
            registeredTables.put(table.getName(), table);
            logger.info("Table enregistrée dans le moteur de requêtes: " + table.getName());
        }
    }
    
    /**
     * Supprime l'enregistrement d'une table
     */
    public void unregisterTable(String tableName) {
        if (registeredTables.remove(tableName) != null) {
            logger.info("Table supprimée du moteur de requêtes: " + tableName);
        }
    }
    
    /**
     * Exécute une requête sur une table
     */
    public QueryResult executeQuery(String tableName, List<String> selectColumns, 
                                   Map<String, Object> filters, String groupBy, 
                                   List<String> orderBy, boolean descending, 
                                   int limit, int offset) {
        
        // Vérifier que la table existe
        Table table = getTable(tableName);
        
        // Construire la requête
        Query.Builder queryBuilder;
        
        if (selectColumns == null || selectColumns.isEmpty()) {
            queryBuilder = Query.selectAll();
        } else {
            queryBuilder = Query.select(selectColumns.toArray(new String[0]));
        }
        
        // Ajouter les filtres
        if (filters != null && !filters.isEmpty()) {
            for (Map.Entry<String, Object> filter : filters.entrySet()) {
                String columnName = filter.getKey();
                Object value = filter.getValue();
                
                // Gérer les conditions complexes (avec opérateur)
                if (value instanceof Map) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> conditionMap = (Map<String, Object>) value;
                    String op = (String) conditionMap.getOrDefault("op", "=");
                    Object conditionValue = conditionMap.get("value");
                    
                    queryBuilder.where(columnName, op, conditionValue);
                } else {
                    // Condition d'égalité simple
                    queryBuilder.where(columnName, "=", value);
                }
            }
        }
        
        // Ajouter le tri
        if (orderBy != null && !orderBy.isEmpty()) {
            for (String column : orderBy) {
                queryBuilder.orderBy(column, descending ? Query.SortDirection.DESC : Query.SortDirection.ASC);
            }
        }
        
        // Ajouter la limite et l'offset
        if (limit > 0) {
            queryBuilder.limit(limit);
        }
        
        if (offset > 0) {
            queryBuilder.offset(offset);
        }
        
        // Exécuter la requête
        Query query = queryBuilder.build();
        logger.info("Exécution de la requête: " + query);
        
        return table.executeQuery(query);
    }
    
    /**
     * Obtient un échantillon d'une table avec une limite spécifiée
     */
    public Table limit(String tableName, int limit, int offset) {
        Table sourceTable = getTable(tableName);
        
        // Créer une requête pour obtenir un échantillon
        Query query = Query.selectAll()
                .limit(limit)
                .offset(offset)
                .build();
        
        // Exécuter la requête
        QueryResult result = sourceTable.executeQuery(query);
        
        // Créer une nouvelle table avec les résultats
        // Note: Ici, nous devrions créer une table temporaire/vue avec les résultats
        // Pour l'instant, nous retournons simplement la table source
        // car l'implémentation complète nécessiterait une classe TableView
        return sourceTable;
    }
    
    /**
     * Exécute une requête SQL
     */
    public QueryResult executeSQL(String sqlQuery) {
        // Ici, nous devrions implémenter un parser SQL
        // Pour l'instant, nous lançons une exception
        throw new ARYDException("L'exécution de requêtes SQL n'est pas encore implémentée", 
                "NOT_IMPLEMENTED", 501);
    }
    
    /**
     * Obtient une table enregistrée
     */
    private Table getTable(String tableName) {
        Table table = registeredTables.get(tableName);
        
        if (table == null) {
            // Essayer de récupérer depuis la base de données
            table = database.getTableOrNull(tableName);
            
            if (table == null) {
                throw new ARYDException("Table non trouvée: " + tableName, "TABLE_NOT_FOUND", 404);
            }
            
            // Enregistrer la table pour les futures requêtes
            registeredTables.put(tableName, table);
        }
        
        return table;
    }
} 
 