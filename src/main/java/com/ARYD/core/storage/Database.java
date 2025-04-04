package com.ARYD.core.storage;

import java.util.List;
import java.util.Optional;
import java.util.Map;

/**
 * Interface pour la base de données qui gère les tables
 */
public interface Database {
    
    /**
     * Obtient le nom de la base de données
     */
    String getName();
    
    /**
     * Crée une nouvelle table
     * 
     * @param tableName Le nom de la table à créer
     * @param columns Les définitions des colonnes de la table
     * @return La table créée
     * @throws IllegalArgumentException si une table avec ce nom existe déjà
     */
    Table createTable(String tableName, List<ColumnDefinition> columns);
    
    /**
     * Crée une nouvelle table vide (sans colonnes)
     * 
     * @param tableName Le nom de la table à créer
     * @return La table créée
     */
    Table createTable(String tableName);
    
    /**
     * Récupère une table par son nom
     * 
     * @param tableName Le nom de la table à récupérer
     * @return Un Optional contenant la table si elle existe, ou vide sinon
     */
    Optional<Table> getTable(String tableName);
    
    /**
     * Récupère une table par son nom (version simplifiée)
     * 
     * @param tableName Le nom de la table à récupérer
     * @return La table ou null si elle n'existe pas
     */
    default Table getTableOrNull(String tableName) {
        return getTable(tableName).orElse(null);
    }
    
    /**
     * Vérifie si une table existe
     * 
     * @param tableName Le nom de la table à vérifier
     * @return true si la table existe, false sinon
     */
    boolean hasTable(String tableName);
    
    /**
     * Supprime une table
     * 
     * @param tableName Le nom de la table à supprimer
     * @return true si la table a été supprimée, false si elle n'existait pas
     */
    boolean dropTable(String tableName);
    
    /**
     * Obtient la liste des noms de toutes les tables
     * 
     * @return La liste des noms de tables
     */
    List<String> getTableNames();
    
    /**
     * Obtient la taille totale de la base de données en mémoire (en octets)
     * 
     * @return La taille approximative en mémoire
     */
    long getMemorySize();
    
    /**
     * Exporte le schéma complet de la base de données
     * 
     * @return Une représentation du schéma
     */
    Object exportSchema();
    
    /**
     * Importe un schéma dans la base de données
     * 
     * @param schema Le schéma à importer
     * @return true si l'importation a réussi
     */
    boolean importSchema(Object schema);
    
    /**
     * Vide toutes les tables (sans les supprimer)
     */
    void truncateAll();
    
    /**
     * Supprime toutes les tables
     */
    void dropAll();
    
    /**
     * Ajoute une table à la base de données
     * 
     * @param table La table à ajouter
     * @return true si la table a été ajoutée, false si une table avec le même nom existe déjà
     */
    default boolean addTable(Table table) {
        if (hasTable(table.getName())) {
            return false;
        }
        // Cette méthode doit être implémentée par les classes concrètes
        return false;
    }
    
    /**
     * Obtient les informations sur les index d'une table
     * 
     * @param tableName Le nom de la table
     * @return Une Map contenant les informations sur les index
     */
    default Map<String, Object> getIndexesForTable(String tableName) {
        return getTableOrNull(tableName).getStatistics();
    }
    
    /**
     * Crée un index sur une table
     * 
     * @param indexName Le nom de l'index
     * @param table La table sur laquelle créer l'index
     * @param columns Les colonnes à indexer
     * @param indexType Le type d'index (facultatif)
     * @return true si l'index a été créé avec succès
     */
    default boolean createIndex(String indexName, Table table, List<String> columns, String indexType) {
        if (table == null || columns == null || columns.isEmpty()) {
            return false;
        }
        
        // Par défaut, créer un index sur chaque colonne individuellement
        boolean allSuccessful = true;
        for (String columnName : columns) {
            if (!table.hasColumn(columnName) || !table.createIndex(columnName)) {
                allSuccessful = false;
            }
        }
        
        return allSuccessful;
    }
} 
 