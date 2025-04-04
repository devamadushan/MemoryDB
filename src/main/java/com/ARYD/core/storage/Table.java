package com.ARYD.core.storage;

import com.ARYD.entity.QueryResult;

import java.util.List;
import java.util.Map;

/**
 * Interface définissant une table dans la base de données en mémoire
 */
public interface Table {
    
    /**
     * Obtient le nom de la table
     */
    String getName();
    
    /**
     * Obtient le nombre total de lignes dans la table
     */
    long getRowCount();
    
    /**
     * Obtient les définitions des colonnes de la table
     */
    List<ColumnDefinition> getColumns();
    
    /**
     * Obtient la définition d'une colonne spécifique par son nom
     * 
     * @param columnName Le nom de la colonne
     * @return La définition de la colonne ou null si elle n'existe pas
     */
    ColumnDefinition getColumn(String columnName);
    
    /**
     * Vérifie si une colonne existe dans la table
     */
    boolean hasColumn(String columnName);
    
    /**
     * Ajoute une valeur à une colonne pour la ligne en cours de construction
     * 
     * @param columnName Le nom de la colonne
     * @param value La valeur à ajouter
     */
    void addValue(String columnName, Object value);
    
    /**
     * Finalise une ligne après avoir ajouté toutes les valeurs
     */
    void finalizeRow();
    
    /**
     * Obtient un échantillon de données de la table
     * 
     * @param limit Le nombre maximum de lignes à retourner
     * @return Une liste de maps représentant les lignes, où chaque map associe le nom de colonne à sa valeur
     */
    List<Map<String, Object>> getSample(int limit);
    
    /**
     * Exécute une requête sur la table
     * 
     * @param query La requête à exécuter
     * @return Le résultat de la requête
     */
    QueryResult executeQuery(Query query);
    
    /**
     * Crée ou met à jour un index sur une colonne
     * 
     * @param columnName Le nom de la colonne à indexer
     * @return true si l'index a été créé ou mis à jour avec succès
     */
    boolean createIndex(String columnName);
    
    /**
     * Supprime un index sur une colonne
     * 
     * @param columnName Le nom de la colonne dont l'index doit être supprimé
     * @return true si l'index a été supprimé avec succès
     */
    boolean dropIndex(String columnName);
    
    /**
     * Vérifie si une colonne est indexée
     */
    boolean isIndexed(String columnName);
    
    /**
     * Obtient des statistiques sur la table
     * 
     * @return Une map contenant des statistiques (nombre de lignes, taille, etc.)
     */
    Map<String, Object> getStatistics();
    
    /**
     * Vide la table (supprime toutes les lignes)
     */
    void truncate();
    
    /**
     * Supprime la table (libère toutes les ressources)
     */
    void drop();
} 