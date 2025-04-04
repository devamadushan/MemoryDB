package com.ARYD.core.storage.impl;

import com.ARYD.core.storage.ColumnDefinition;
import com.ARYD.core.storage.Query;
import com.ARYD.core.storage.Table;
import com.ARYD.core.types.DataType;
import com.ARYD.entity.QueryResult;
import com.ARYD.exception.ARYDException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * Implémentation en mémoire d'une table
 */
public class InMemoryTable implements Table {

    private static final Logger logger = Logger.getLogger(InMemoryTable.class.getName());
    
    private final String name;
    private final List<ColumnDefinition> columns;
    private final Map<String, Integer> columnIndexes;
    private final List<Map<String, Object>> data;
    private final Map<String, Map<Object, List<Integer>>> indexes;
    
    private final AtomicLong rowCount;
    private Map<String, Object> currentRow;
    
    /**
     * Constructeur pour créer une table avec des colonnes définies
     */
    public InMemoryTable(String name, List<ColumnDefinition> columns) {
        this.name = name;
        this.columns = new ArrayList<>(columns);
        this.columnIndexes = new HashMap<>();
        this.data = Collections.synchronizedList(new ArrayList<>());
        this.indexes = new ConcurrentHashMap<>();
        this.rowCount = new AtomicLong(0);
        
        // Initialiser les indexes des colonnes
        for (int i = 0; i < columns.size(); i++) {
            ColumnDefinition column = columns.get(i);
            columnIndexes.put(column.getName(), i);
            
            // Créer les index pour les colonnes marquées comme indexées
            if (column.isIndexed()) {
                createIndex(column.getName());
            }
        }
        
        logger.info("Table créée: " + name + " avec " + columns.size() + " colonnes");
    }
    
    /**
     * Constructeur pour créer une table sans colonnes prédéfinies (les détectera dynamiquement)
     */
    public InMemoryTable(String name) {
        this(name, new ArrayList<>());
    }
    
    @Override
    public String getName() {
        return name;
    }
    
    @Override
    public long getRowCount() {
        return rowCount.get();
    }
    
    @Override
    public List<ColumnDefinition> getColumns() {
        return Collections.unmodifiableList(columns);
    }
    
    @Override
    public ColumnDefinition getColumn(String columnName) {
        Integer index = columnIndexes.get(columnName);
        if (index != null) {
            return columns.get(index);
        }
        return null;
    }
    
    @Override
    public boolean hasColumn(String columnName) {
        return columnIndexes.containsKey(columnName);
    }
    
    /**
     * Ajoute dynamiquement une nouvelle colonne si elle n'existe pas
     */
    private void addColumnIfNotExists(String columnName, Object value) {
        if (!hasColumn(columnName)) {
            // Détecter le type de la colonne à partir de la valeur
            String typeName = DataType.inferType(value);
            
            // Créer la définition de colonne
            ColumnDefinition column = new ColumnDefinition(columnName, typeName, true, false);
            columns.add(column);
            columnIndexes.put(columnName, columns.size() - 1);
            
            logger.info("Nouvelle colonne détectée et ajoutée: " + columnName + " de type " + typeName);
        }
    }
    
    @Override
    public void addValue(String columnName, Object value) {
        // Créer une nouvelle ligne si nécessaire
        if (currentRow == null) {
            currentRow = new HashMap<>();
        }
        
        // Ajouter la colonne si elle n'existe pas encore
        if (!hasColumn(columnName)) {
            addColumnIfNotExists(columnName, value);
        }
        
        // Stocker la valeur (peut être null)
        currentRow.put(columnName, value);
    }
    
    @Override
    public void finalizeRow() {
        if (currentRow != null) {
            // Vérifier que toutes les colonnes requises ont des valeurs
            for (ColumnDefinition column : columns) {
                if (!column.isNullable() && !currentRow.containsKey(column.getName())) {
                    throw new ARYDException("La colonne non-nullable '" + column.getName() + 
                            "' n'a pas de valeur", "NULL_CONSTRAINT_VIOLATION");
                }
            }
            
            // Ajouter la ligne aux données
            int rowIndex = data.size();
            data.add(currentRow);
            
            // Mettre à jour les index pour cette ligne
            for (String columnName : indexes.keySet()) {
                if (currentRow.containsKey(columnName)) {
                    Object value = currentRow.get(columnName);
                    Map<Object, List<Integer>> columnIndex = indexes.get(columnName);
                    
                    columnIndex.computeIfAbsent(value, k -> new ArrayList<>()).add(rowIndex);
                }
            }
            
            // Incrémenter le compteur de lignes
            rowCount.incrementAndGet();
            
            // Réinitialiser la ligne courante
            currentRow = null;
            
            // Log périodique du nombre de lignes
            if (rowCount.get() % 100000 == 0) {
                logger.info("Table " + name + ": " + rowCount.get() + " lignes chargées");
            }
        }
    }
    
    @Override
    public List<Map<String, Object>> getSample(int limit) {
        if (limit <= 0) {
            return Collections.emptyList();
        }
        
        int size = Math.min(limit, data.size());
        return Collections.unmodifiableList(data.subList(0, size));
    }
    
    @Override
    public QueryResult executeQuery(Query query) {
        long startTime = System.currentTimeMillis();
        
        List<Map<String, Object>> resultRows = new ArrayList<>();
        Map<String, Object> statistics = new HashMap<>();
        
        // Filtrer les lignes selon les conditions
        List<Integer> matchingRowIndexes = getMatchingRowIndexes(query.getConditions());
        
        // Appliquer la clause DISTINCT si nécessaire
        if (query.isDistinct()) {
            matchingRowIndexes = applyDistinct(matchingRowIndexes, query.getSelectColumns());
        }
        
        // Appliquer le tri si spécifié
        if (!query.getSortCriteria().isEmpty()) {
            applySorting(matchingRowIndexes, query.getSortCriteria());
        }
        
        // Calculer le nombre total de lignes correspondantes
        int totalMatchingRows = matchingRowIndexes.size();
        
        // Appliquer la pagination (LIMIT et OFFSET)
        int offset = Math.max(0, query.getOffset());
        int limit = query.getLimit() >= 0 ? query.getLimit() : Integer.MAX_VALUE;
        
        // Vérifier que l'offset est valide
        if (offset >= totalMatchingRows) {
            offset = 0;
        }
        
        // Calculer l'index de fin
        int endIndex = Math.min(offset + limit, totalMatchingRows);
        
        // Si on demande juste le nombre de lignes, pas besoin de récupérer les données
        if (query.isReturnRowCount()) {
            // Construction du résultat final
            QueryResult result = new QueryResult();
            result.setTableName(name);
            result.setTotalRows(totalMatchingRows);
            result.setReturnedRows(0);
            result.setOffset(offset);
            result.setExecutionTimeMs(System.currentTimeMillis() - startTime);
            
            Map<String, Object> countResult = new HashMap<>();
            countResult.put("count", totalMatchingRows);
            resultRows.add(countResult);
            
            result.setRows(resultRows);
            return result;
        }
        
        // Sélectionner les colonnes requises
        List<String> columnsToSelect = query.isSelectAll() ? 
                columns.stream().map(ColumnDefinition::getName).collect(Collectors.toList()) : 
                query.getSelectColumns();
        
        // Extraire les données pour les lignes sélectionnées
        for (int i = offset; i < endIndex; i++) {
            int rowIndex = matchingRowIndexes.get(i);
            Map<String, Object> row = data.get(rowIndex);
            
            // Filtrer les colonnes si nécessaire
            if (!query.isSelectAll()) {
                Map<String, Object> filteredRow = new LinkedHashMap<>();
                for (String columnName : columnsToSelect) {
                    if (row.containsKey(columnName)) {
                        filteredRow.put(columnName, row.get(columnName));
                    } else {
                        filteredRow.put(columnName, null);
                    }
                }
                resultRows.add(filteredRow);
            } else {
                resultRows.add(new LinkedHashMap<>(row));
            }
        }
        
        // Rassembler les statistiques
        statistics.put("filteredRowCount", totalMatchingRows);
        statistics.put("processingTimeMs", System.currentTimeMillis() - startTime);
        
        // Construction du résultat final
        QueryResult result = new QueryResult();
        result.setTableName(name);
        result.setTotalRows(totalMatchingRows);
        result.setReturnedRows(resultRows.size());
        result.setOffset(offset);
        result.setColumns(columnsToSelect);
        result.setRows(resultRows);
        result.setStatistics(statistics);
        result.setExecutionTimeMs(System.currentTimeMillis() - startTime);
        
        return result;
    }
    
    /**
     * Trouve les index des lignes qui correspondent aux conditions de la requête
     */
    private List<Integer> getMatchingRowIndexes(List<Query.Condition> conditions) {
        if (conditions.isEmpty()) {
            // Si pas de conditions, retourner toutes les lignes
            List<Integer> allIndexes = new ArrayList<>(data.size());
            for (int i = 0; i < data.size(); i++) {
                allIndexes.add(i);
            }
            return allIndexes;
        }
        
        // Les conditions sont combinées avec l'opérateur logique approprié (AND ou OR)
        List<Integer> result = null;
        
        for (Query.Condition condition : conditions) {
            // Trouver les lignes qui correspondent à cette condition
            List<Integer> matchingForCondition = findRowsMatchingCondition(condition);
            
            if (result == null) {
                // Première condition
                result = matchingForCondition;
            } else {
                // Combiner avec le résultat précédent selon l'opérateur logique
                if (condition.getNextOperator() == Query.LogicalOperator.AND) {
                    // Intersection (AND)
                    result.retainAll(matchingForCondition);
                } else {
                    // Union (OR)
                    for (Integer index : matchingForCondition) {
                        if (!result.contains(index)) {
                            result.add(index);
                        }
                    }
                }
            }
        }
        
        return result;
    }
    
    /**
     * Trouve les lignes qui correspondent à une condition spécifique
     */
    private List<Integer> findRowsMatchingCondition(Query.Condition condition) {
        String columnName = condition.getColumnName();
        Query.Operation operation = condition.getOperation();
        Object value = condition.getValue();
        Object secondValue = condition.getSecondValue();
        
        // Vérifier si le colonne existe
        if (!hasColumn(columnName)) {
            throw new ARYDException("Colonne inconnue: " + columnName, "UNKNOWN_COLUMN");
        }
        
        // Vérifier d'abord si un index existe pour cette colonne et si l'opération peut l'utiliser
        if (indexes.containsKey(columnName) && canUseIndexForOperation(operation)) {
            return findRowsUsingIndex(columnName, operation, value, secondValue);
        }
        
        // Scan complet (moins efficace)
        List<Integer> matchingRows = new ArrayList<>();
        
        for (int i = 0; i < data.size(); i++) {
            Map<String, Object> row = data.get(i);
            
            if (matchesCondition(row, columnName, operation, value, secondValue)) {
                matchingRows.add(i);
            }
        }
        
        return matchingRows;
    }
    
    /**
     * Vérifie si une opération peut utiliser un index
     */
    private boolean canUseIndexForOperation(Query.Operation operation) {
        switch (operation) {
            case EQUALS:
            case IN:
            case IS_NULL:
            case IS_NOT_NULL:
                return true;
            default:
                return false;
        }
    }
    
    /**
     * Utilise un index pour trouver les lignes correspondantes
     */
    private List<Integer> findRowsUsingIndex(String columnName, Query.Operation operation, 
                                          Object value, Object secondValue) {
        Map<Object, List<Integer>> columnIndex = indexes.get(columnName);
        
        switch (operation) {
            case EQUALS:
                return columnIndex.getOrDefault(value, Collections.emptyList());
                
            case IN:
                if (value instanceof List) {
                    List<?> valuesList = (List<?>) value;
                    List<Integer> result = new ArrayList<>();
                    
                    for (Object val : valuesList) {
                        result.addAll(columnIndex.getOrDefault(val, Collections.emptyList()));
                    }
                    
                    return result;
                }
                break;
                
            case IS_NULL:
                return columnIndex.getOrDefault(null, Collections.emptyList());
                
            case IS_NOT_NULL:
                List<Integer> allRows = new ArrayList<>();
                for (Object key : columnIndex.keySet()) {
                    if (key != null) {
                        allRows.addAll(columnIndex.get(key));
                    }
                }
                return allRows;
        }
        
        // Fallback au scan complet
        return findRowsMatchingCondition(new Query.Condition(
                columnName, operation, value, secondValue, Query.LogicalOperator.AND));
    }
    
    /**
     * Vérifie si une ligne correspond à une condition
     */
    private boolean matchesCondition(Map<String, Object> row, String columnName, 
                                  Query.Operation operation, Object value, Object secondValue) {
        // Obtenir la valeur de la colonne dans cette ligne
        Object columnValue = row.get(columnName);
        
        // Opérations sur les valeurs null
        if (operation == Query.Operation.IS_NULL) {
            return columnValue == null;
        } else if (operation == Query.Operation.IS_NOT_NULL) {
            return columnValue != null;
        }
        
        // Si la valeur de la colonne est null, elle ne peut pas correspondre aux autres opérations
        if (columnValue == null) {
            return false;
        }
        
        // Obtenir la définition de la colonne pour connaître son type
        ColumnDefinition column = getColumn(columnName);
        String typeName = column.getTypeName();
        
        // Convertir la valeur de comparaison au type de la colonne
        Object convertedValue = value;
        Object convertedSecondValue = secondValue;
        
        try {
            if (value != null) {
                convertedValue = DataType.convertToType(value, typeName);
            }
            
            if (secondValue != null) {
                convertedSecondValue = DataType.convertToType(secondValue, typeName);
            }
        } catch (IllegalArgumentException e) {
            // Si la conversion échoue, considérer que la condition ne correspond pas
            logger.warning("Erreur de conversion de type: " + e.getMessage());
            return false;
        }
        
        // Comparer selon l'opération
        switch (operation) {
            case EQUALS:
                return Objects.equals(columnValue, convertedValue);
                
            case NOT_EQUALS:
                return !Objects.equals(columnValue, convertedValue);
                
            case GREATER_THAN:
                return compareValues(columnValue, convertedValue) > 0;
                
            case LESS_THAN:
                return compareValues(columnValue, convertedValue) < 0;
                
            case GREATER_EQUALS:
                return compareValues(columnValue, convertedValue) >= 0;
                
            case LESS_EQUALS:
                return compareValues(columnValue, convertedValue) <= 0;
                
            case LIKE:
            case CONTAINS:
                return columnValue.toString().contains(convertedValue.toString());
                
            case STARTS_WITH:
                return columnValue.toString().startsWith(convertedValue.toString());
                
            case ENDS_WITH:
                return columnValue.toString().endsWith(convertedValue.toString());
                
            case IN:
                if (convertedValue instanceof List) {
                    List<?> valuesList = (List<?>) convertedValue;
                    return valuesList.contains(columnValue);
                }
                return false;
                
            case NOT_IN:
                if (convertedValue instanceof List) {
                    List<?> valuesList = (List<?>) convertedValue;
                    return !valuesList.contains(columnValue);
                }
                return true;
                
            case BETWEEN:
                return compareValues(columnValue, convertedValue) >= 0 && 
                       compareValues(columnValue, convertedSecondValue) <= 0;
                
            default:
                return false;
        }
    }
    
    /**
     * Compare deux valeurs
     * 
     * @return un entier négatif, zéro ou positif si la première valeur est inférieure, égale ou supérieure à la seconde
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private int compareValues(Object value1, Object value2) {
        // Si les deux valeurs sont comparables et de même type
        if (value1 instanceof Comparable && value1.getClass().isInstance(value2)) {
            return ((Comparable) value1).compareTo(value2);
        }
        
        // Sinon, comparer les représentations en chaîne
        return value1.toString().compareTo(value2.toString());
    }
    
    /**
     * Applique la clause DISTINCT
     */
    private List<Integer> applyDistinct(List<Integer> rowIndexes, List<String> columns) {
        // Si aucune colonne n'est spécifiée, utiliser toutes les colonnes
        List<String> distinctColumns = columns.isEmpty() ? 
                this.columns.stream().map(ColumnDefinition::getName).collect(Collectors.toList()) : 
                columns;
        
        // Utiliser un Set pour éliminer les doublons
        Map<String, Integer> distinctValues = new HashMap<>();
        List<Integer> distinctIndexes = new ArrayList<>();
        
        for (Integer rowIndex : rowIndexes) {
            Map<String, Object> row = data.get(rowIndex);
            
            // Construire une clé unique pour cette ligne basée sur les colonnes sélectionnées
            StringBuilder keyBuilder = new StringBuilder();
            for (String column : distinctColumns) {
                keyBuilder.append(column).append("=").append(row.get(column)).append("|");
            }
            
            String key = keyBuilder.toString();
            
            // Ajouter l'index si cette combinaison de valeurs n'a pas déjà été vue
            if (!distinctValues.containsKey(key)) {
                distinctValues.put(key, rowIndex);
                distinctIndexes.add(rowIndex);
            }
        }
        
        return distinctIndexes;
    }
    
    /**
     * Applique le tri sur les lignes
     */
    private void applySorting(List<Integer> rowIndexes, List<Query.SortCriteria> sortCriteria) {
        Collections.sort(rowIndexes, (a, b) -> {
            Map<String, Object> rowA = data.get(a);
            Map<String, Object> rowB = data.get(b);
            
            for (Query.SortCriteria criteria : sortCriteria) {
                String columnName = criteria.getColumnName();
                boolean ascending = criteria.getDirection() == Query.SortDirection.ASC;
                
                Object valueA = rowA.get(columnName);
                Object valueB = rowB.get(columnName);
                
                // Gestion des valeurs null
                if (valueA == null && valueB == null) {
                    continue;
                } else if (valueA == null) {
                    return ascending ? -1 : 1;
                } else if (valueB == null) {
                    return ascending ? 1 : -1;
                }
                
                // Comparaison des valeurs
                int comparison = compareValues(valueA, valueB);
                
                // Inverser si tri descendant
                if (!ascending) {
                    comparison = -comparison;
                }
                
                if (comparison != 0) {
                    return comparison;
                }
            }
            
            return 0;
        });
    }
    
    @Override
    public boolean createIndex(String columnName) {
        if (!hasColumn(columnName)) {
            throw new ARYDException("Impossible de créer un index: colonne inconnue: " + columnName, 
                    "UNKNOWN_COLUMN");
        }
        
        if (indexes.containsKey(columnName)) {
            // L'index existe déjà
            return false;
        }
        
        // Créer l'index
        Map<Object, List<Integer>> columnIndex = new ConcurrentHashMap<>();
        indexes.put(columnName, columnIndex);
        
        // Remplir l'index avec les données existantes
        for (int i = 0; i < data.size(); i++) {
            Map<String, Object> row = data.get(i);
            Object value = row.get(columnName);
            
            columnIndex.computeIfAbsent(value, k -> new ArrayList<>()).add(i);
        }
        
        // Mettre à jour la définition de la colonne pour la marquer comme indexée
        for (int i = 0; i < columns.size(); i++) {
            ColumnDefinition column = columns.get(i);
            if (column.getName().equals(columnName) && !column.isIndexed()) {
                // Créer une nouvelle définition avec indexation activée
                ColumnDefinition newDef = new ColumnDefinition(
                        column.getName(), column.getTypeName(), column.isNullable(), true);
                
                // Copier les attributs optionnels
                if (column.getDescription() != null) {
                    newDef.setDescription(column.getDescription());
                }
                if (column.getFormat() != null) {
                    newDef.setFormat(column.getFormat());
                }
                if (column.getDefaultValue() != null) {
                    newDef.setDefaultValue(column.getDefaultValue());
                }
                
                columns.set(i, newDef);
                break;
            }
        }
        
        logger.info("Index créé sur la colonne '" + columnName + "' pour la table '" + name + "'");
        return true;
    }
    
    @Override
    public boolean dropIndex(String columnName) {
        if (!indexes.containsKey(columnName)) {
            return false;
        }
        
        // Supprimer l'index
        indexes.remove(columnName);
        
        // Mettre à jour la définition de la colonne
        for (int i = 0; i < columns.size(); i++) {
            ColumnDefinition column = columns.get(i);
            if (column.getName().equals(columnName) && column.isIndexed()) {
                // Créer une nouvelle définition avec indexation désactivée
                ColumnDefinition newDef = new ColumnDefinition(
                        column.getName(), column.getTypeName(), column.isNullable(), false);
                
                // Copier les attributs optionnels
                if (column.getDescription() != null) {
                    newDef.setDescription(column.getDescription());
                }
                if (column.getFormat() != null) {
                    newDef.setFormat(column.getFormat());
                }
                if (column.getDefaultValue() != null) {
                    newDef.setDefaultValue(column.getDefaultValue());
                }
                
                columns.set(i, newDef);
                break;
            }
        }
        
        logger.info("Index supprimé sur la colonne '" + columnName + "' pour la table '" + name + "'");
        return true;
    }
    
    @Override
    public boolean isIndexed(String columnName) {
        return indexes.containsKey(columnName);
    }
    
    @Override
    public Map<String, Object> getStatistics() {
        Map<String, Object> stats = new HashMap<>();
        stats.put("name", name);
        stats.put("rowCount", rowCount.get());
        stats.put("columnCount", columns.size());
        stats.put("indexCount", indexes.size());
        
        // Calculer la taille approximative en mémoire
        long approxSize = 0;
        
        // Taille des métadonnées
        approxSize += 100; // constantes diverses
        approxSize += name.length() * 2; // String
        approxSize += columns.size() * 50; // estimation ColumnDefinition
        
        // Taille des données
        for (Map<String, Object> row : data) {
            long rowSize = 16; // overhead Map
            
            for (Map.Entry<String, Object> entry : row.entrySet()) {
                rowSize += entry.getKey().length() * 2; // String clé
                
                Object value = entry.getValue();
                if (value != null) {
                    if (value instanceof String) {
                        rowSize += ((String) value).length() * 2;
                    } else if (value instanceof Number) {
                        rowSize += 8; // Long, Double etc.
                    } else if (value instanceof Boolean) {
                        rowSize += 1;
                    } else {
                        rowSize += 16; // default size pour les autres objets
                    }
                }
            }
            
            approxSize += rowSize;
        }
        
        // Taille des index
        for (Map<Object, List<Integer>> index : indexes.values()) {
            approxSize += 16; // overhead Map
            
            for (Map.Entry<Object, List<Integer>> entry : index.entrySet()) {
                Object key = entry.getKey();
                if (key != null) {
                    if (key instanceof String) {
                        approxSize += ((String) key).length() * 2;
                    } else {
                        approxSize += 8; // estimation pour les autres types
                    }
                }
                
                List<Integer> values = entry.getValue();
                approxSize += 16 + values.size() * 4; // overhead List + integers
            }
        }
        
        stats.put("memorySize", approxSize);
        
        // Statistiques par colonne
        List<Map<String, Object>> columnStats = new ArrayList<>();
        for (ColumnDefinition column : columns) {
            Map<String, Object> colStat = new HashMap<>();
            colStat.put("name", column.getName());
            colStat.put("type", column.getTypeName());
            colStat.put("nullable", column.isNullable());
            colStat.put("indexed", column.isIndexed());
            
            // Statistiques additionnelles si données présentes
            if (!data.isEmpty()) {
                int nullCount = 0;
                Object minValue = null;
                Object maxValue = null;
                Set<Object> distinctValues = new HashSet<>();
                
                for (Map<String, Object> row : data) {
                    Object value = row.get(column.getName());
                    if (value == null) {
                        nullCount++;
                    } else {
                        distinctValues.add(value);
                        
                        if (minValue == null || compareValues(value, minValue) < 0) {
                            minValue = value;
                        }
                        if (maxValue == null || compareValues(value, maxValue) > 0) {
                            maxValue = value;
                        }
                    }
                }
                
                colStat.put("nullCount", nullCount);
                colStat.put("distinctCount", distinctValues.size());
                
                if (minValue != null) {
                    colStat.put("minValue", minValue);
                }
                if (maxValue != null) {
                    colStat.put("maxValue", maxValue);
                }
            }
            
            columnStats.add(colStat);
        }
        
        stats.put("columns", columnStats);
        
        return stats;
    }
    
    @Override
    public void truncate() {
        data.clear();
        
        // Réinitialiser les index
        for (String columnName : indexes.keySet()) {
            indexes.put(columnName, new ConcurrentHashMap<>());
        }
        
        rowCount.set(0);
        logger.info("Table vidée: " + name);
    }
    
    @Override
    public void drop() {
        truncate();
        columns.clear();
        columnIndexes.clear();
        indexes.clear();
        logger.info("Table détruite: " + name);
    }
    
    @Override
    public String toString() {
        return "InMemoryTable[name=" + name + ", rows=" + rowCount.get() + ", columns=" + columns.size() + "]";
    }
} 