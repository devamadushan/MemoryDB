package com.ARYD.MemoryDB.query;

import com.ARYD.MemoryDB.storage.ColumnDefinition;
import com.ARYD.MemoryDB.storage.Table;
import com.ARYD.MemoryDB.types.DataType;
import com.ARYD.MemoryDB.types.Operator;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Moteur de requêtes pour exécuter des opérations de type SQL sur les tables en mémoire
 */
public class QueryEngine {

    private final Map<String, Table> tables;
    
    /**
     * Constructeur
     */
    public QueryEngine() {
        this.tables = new ConcurrentHashMap<>();
    }
    
    /**
     * Enregistre une table dans le moteur de requêtes
     * @param table Table à enregistrer
     */
    public void registerTable(Table table) {
        tables.put(table.getName(), table);
    }
    
    /**
     * Supprime une table du moteur de requêtes
     * @param tableName Nom de la table à supprimer
     */
    public void unregisterTable(String tableName) {
        tables.remove(tableName);
    }
    
    /**
     * Retourne une table par son nom
     * @param tableName Nom de la table
     * @return Table correspondante
     */
    public Table getTable(String tableName) {
        Table table = tables.get(tableName);
        if (table == null) {
            throw new IllegalArgumentException("Table inconnue: " + tableName);
        }
        return table;
    }
    
    /**
     * Exécute une requête de sélection simple avec une condition
     * @param tableName Nom de la table
     * @param columnName Nom de la colonne pour la condition
     * @param operator Opérateur de comparaison
     * @param value Valeur de référence
     * @return Table résultat contenant les lignes correspondant à la condition
     */
    public Table select(String tableName, String columnName, Operator operator, Object value) {
        Table table = getTable(tableName);
        
        // Pour EQUALS, on peut utiliser directement la méthode filter
        if (operator == Operator.EQUALS) {
            return table.filter(columnName, value);
        }
        
        // Pour les autres opérateurs, parcourir séquentiellement
        // Créer une nouvelle table avec les mêmes définitions de colonnes
        List<ColumnDefinition> columnDefs = table.getColumnDefinitions();
        Table result = new Table(tableName + "_result", columnDefs);
        
        // Appliquer la condition sur chaque ligne
        for (int i = 0; i < table.getRowCount(); i++) {
            Map<String, Object> row = table.getRow(i);
            Object cellValue = row.get(columnName);
            
            if (compareValues(cellValue, value, operator)) {
                result.addRow(row);
            }
        }
        
        return result;
    }
    
    /**
     * Compare deux valeurs selon l'opérateur spécifié
     * @param left Valeur gauche
     * @param right Valeur droite
     * @param operator Opérateur de comparaison
     * @return true si la condition est satisfaite, false sinon
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private boolean compareValues(Object left, Object right, Operator operator) {
        if (left == null && right == null) {
            return operator == Operator.EQUALS;
        }
        if (left == null || right == null) {
            return operator == Operator.NOT_EQUALS;
        }
        
        // Si les types sont différents, essayer de convertir
        if (!left.getClass().equals(right.getClass())) {
            if (left instanceof Number && right instanceof Number) {
                // Convertir en double pour la comparaison
                double leftDouble = ((Number) left).doubleValue();
                double rightDouble = ((Number) right).doubleValue();
                return compareDoubles(leftDouble, rightDouble, operator);
            } else if (left instanceof String || right instanceof String) {
                // Convertir en String pour la comparaison
                String leftStr = left.toString();
                String rightStr = right.toString();
                return compareStrings(leftStr, rightStr, operator);
            }
        }
        
        // Même type, comparer directement
        if (left instanceof Number) {
            double leftDouble = ((Number) left).doubleValue();
            double rightDouble = ((Number) right).doubleValue();
            return compareDoubles(leftDouble, rightDouble, operator);
        } else if (left instanceof String) {
            return compareStrings((String) left, (String) right, operator);
        } else if (left instanceof Comparable) {
            return compareComparables((Comparable) left, (Comparable) right, operator);
        }
        
        // Fallback sur equals pour les autres types
        switch (operator) {
            case EQUALS:
                return left.equals(right);
            case NOT_EQUALS:
                return !left.equals(right);
            default:
                throw new UnsupportedOperationException("Opérateur non supporté pour ce type: " + operator);
        }
    }
    
    /**
     * Compare deux doubles selon l'opérateur spécifié
     */
    private boolean compareDoubles(double left, double right, Operator operator) {
        switch (operator) {
            case EQUALS:
                return Math.abs(left - right) < 1e-10;
            case NOT_EQUALS:
                return Math.abs(left - right) >= 1e-10;
            case GREATER:
                return left > right;
            case GREATER_OR_EQUALS:
                return left >= right;
            case LESS:
                return left < right;
            case LESS_OR_EQUALS:
                return left <= right;
            default:
                throw new UnsupportedOperationException("Opérateur non supporté pour les nombres: " + operator);
        }
    }
    
    /**
     * Compare deux chaînes selon l'opérateur spécifié
     */
    private boolean compareStrings(String left, String right, Operator operator) {
        switch (operator) {
            case EQUALS:
                return left.equals(right);
            case NOT_EQUALS:
                return !left.equals(right);
            case GREATER:
                return left.compareTo(right) > 0;
            case GREATER_OR_EQUALS:
                return left.compareTo(right) >= 0;
            case LESS:
                return left.compareTo(right) < 0;
            case LESS_OR_EQUALS:
                return left.compareTo(right) <= 0;
            case LIKE:
                // Convertir le pattern SQL LIKE en regex
                String regex = right.replace("%", ".*").replace("_", ".");
                return left.matches(regex);
            case CONTAINS:
                return left.contains(right);
            case STARTS_WITH:
                return left.startsWith(right);
            case ENDS_WITH:
                return left.endsWith(right);
            default:
                throw new UnsupportedOperationException("Opérateur non supporté pour les chaînes: " + operator);
        }
    }
    
    /**
     * Compare deux Comparable selon l'opérateur spécifié
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private boolean compareComparables(Comparable left, Comparable right, Operator operator) {
        int comparison;
        try {
            comparison = left.compareTo(right);
        } catch (ClassCastException e) {
            throw new IllegalArgumentException("Impossible de comparer " + left + " et " + right, e);
        }
        
        switch (operator) {
            case EQUALS:
                return comparison == 0;
            case NOT_EQUALS:
                return comparison != 0;
            case GREATER:
                return comparison > 0;
            case GREATER_OR_EQUALS:
                return comparison >= 0;
            case LESS:
                return comparison < 0;
            case LESS_OR_EQUALS:
                return comparison <= 0;
            default:
                throw new UnsupportedOperationException("Opérateur non supporté pour ce type: " + operator);
        }
    }
    
    /**
     * Effectue une projection (sélection de colonnes) sur une table
     * @param tableName Nom de la table
     * @param columnNames Liste des noms de colonnes à inclure
     * @return Table résultat avec uniquement les colonnes spécifiées
     */
    public Table project(String tableName, List<String> columnNames) {
        Table table = getTable(tableName);
        
        // Vérifier que toutes les colonnes existent
        List<String> tableColumns = table.getColumnNames();
        for (String column : columnNames) {
            if (!tableColumns.contains(column)) {
                throw new IllegalArgumentException("Colonne inconnue: " + column);
            }
        }
        
        // Créer la table résultat avec les définitions de colonnes sélectionnées
        List<ColumnDefinition> selectedColumnDefs = table.getColumnDefinitions().stream()
            .filter(def -> columnNames.contains(def.getName()))
            .collect(Collectors.toList());
        
        Table result = new Table(tableName + "_projection", selectedColumnDefs);
        
        // Copier les données
        for (int i = 0; i < table.getRowCount(); i++) {
            Map<String, Object> row = table.getRow(i);
            
            // Filtrer les colonnes
            Map<String, Object> projectedRow = new HashMap<>();
            for (String column : columnNames) {
                projectedRow.put(column, row.get(column));
            }
            
            result.addRow(projectedRow);
        }
        
        return result;
    }
    
    /**
     * Applique un limite et un décalage à une table (pour la pagination)
     * @param tableName Nom de la table
     * @param limit Nombre maximum de lignes à retourner
     * @param offset Décalage (nombre de lignes à ignorer)
     * @return Table résultat limitée
     */
    public Table limit(String tableName, int limit, int offset) {
        Table table = getTable(tableName);
        
        // Créer la table résultat avec les mêmes définitions de colonnes
        List<ColumnDefinition> columnDefs = table.getColumnDefinitions();
        Table result = new Table(tableName + "_limited", columnDefs);
        
        // Calculer l'intervalle de lignes à inclure
        int startRow = Math.min(offset, table.getRowCount());
        int endRow = Math.min(offset + limit, table.getRowCount());
        
        // Copier uniquement les lignes dans l'intervalle
        for (int i = startRow; i < endRow; i++) {
            Map<String, Object> row = table.getRow(i);
            result.addRow(row);
        }
        
        return result;
    }
    
    /**
     * Effectue une jointure interne entre deux tables
     * @param leftTableName Nom de la table de gauche
     * @param rightTableName Nom de la table de droite
     * @param leftColumn Colonne de jointure dans la table de gauche
     * @param rightColumn Colonne de jointure dans la table de droite
     * @return Table résultat de la jointure
     */
    public Table join(String leftTableName, String rightTableName, String leftColumn, String rightColumn) {
        Table leftTable = getTable(leftTableName);
        Table rightTable = getTable(rightTableName);
        
        // Vérifier que les colonnes existent
        if (!leftTable.getColumnNames().contains(leftColumn)) {
            throw new IllegalArgumentException("Colonne inconnue dans la table de gauche: " + leftColumn);
        }
        if (!rightTable.getColumnNames().contains(rightColumn)) {
            throw new IllegalArgumentException("Colonne inconnue dans la table de droite: " + rightColumn);
        }
        
        // Créer des définitions de colonnes pour la table résultante
        List<ColumnDefinition> joinedColumnDefs = new ArrayList<>();
        
        // Ajouter les colonnes de la table de gauche
        for (ColumnDefinition leftColDef : leftTable.getColumnDefinitions()) {
            joinedColumnDefs.add(new ColumnDefinition(
                leftTableName + "." + leftColDef.getName(),
                leftColDef.getType(),
                leftColDef.isNullable(),
                leftColDef.isIndexed()
            ));
        }
        
        // Ajouter les colonnes de la table de droite (sauf celle de jointure si même nom)
        for (ColumnDefinition rightColDef : rightTable.getColumnDefinitions()) {
            if (!rightColDef.getName().equals(rightColumn) || !leftColumn.equals(rightColumn)) {
                joinedColumnDefs.add(new ColumnDefinition(
                    rightTableName + "." + rightColDef.getName(),
                    rightColDef.getType(),
                    rightColDef.isNullable(),
                    rightColDef.isIndexed()
                ));
            }
        }
        
        // Créer la table résultat
        Table result = new Table(leftTableName + "_join_" + rightTableName, joinedColumnDefs);
        
        // Construire un index pour la table de droite si ce n'est pas déjà fait
        Map<Object, List<Integer>> rightIndex = new HashMap<>();
        for (int i = 0; i < rightTable.getRowCount(); i++) {
            Object value = rightTable.getRow(i).get(rightColumn);
            rightIndex.computeIfAbsent(value, k -> new ArrayList<>()).add(i);
        }
        
        // Effectuer la jointure
        for (int leftIdx = 0; leftIdx < leftTable.getRowCount(); leftIdx++) {
            Map<String, Object> leftRow = leftTable.getRow(leftIdx);
            Object leftValue = leftRow.get(leftColumn);
            
            // Trouver les lignes correspondantes dans la table de droite
            List<Integer> rightIndices = rightIndex.getOrDefault(leftValue, Collections.emptyList());
            
            for (int rightIdx : rightIndices) {
                Map<String, Object> rightRow = rightTable.getRow(rightIdx);
                
                // Fusionner les lignes
                Map<String, Object> joinedRow = new HashMap<>();
                
                // Ajouter les valeurs de la table de gauche
                for (String column : leftTable.getColumnNames()) {
                    joinedRow.put(leftTableName + "." + column, leftRow.get(column));
                }
                
                // Ajouter les valeurs de la table de droite
                for (String column : rightTable.getColumnNames()) {
                    if (!column.equals(rightColumn) || !leftColumn.equals(rightColumn)) {
                        joinedRow.put(rightTableName + "." + column, rightRow.get(column));
                    }
                }
                
                result.addRow(joinedRow);
            }
        }
        
        return result;
    }
    
    /**
     * Applique une fonction d'agrégation sur une colonne
     * @param tableName Nom de la table
     * @param columnName Nom de la colonne
     * @param function Fonction d'agrégation (sum, avg, min, max, count)
     * @return Résultat de l'agrégation
     */
    public Object aggregate(String tableName, String columnName, String function) {
        Table table = getTable(tableName);
        
        if (!table.getColumnNames().contains(columnName)) {
            throw new IllegalArgumentException("Colonne inconnue: " + columnName);
        }
        
        List<Object> values = new ArrayList<>();
        for (int i = 0; i < table.getRowCount(); i++) {
            Object value = table.getRow(i).get(columnName);
            if (value != null) {
                values.add(value);
            }
        }
        
        if (values.isEmpty()) {
            return null;
        }
        
        switch (function.toLowerCase()) {
            case "count":
                return values.size();
                
            case "sum":
                if (values.get(0) instanceof Number) {
                    return values.stream()
                            .mapToDouble(v -> ((Number) v).doubleValue())
                            .sum();
                }
                throw new IllegalArgumentException("La fonction SUM ne peut être appliquée qu'à des nombres");
                
            case "avg":
                if (values.get(0) instanceof Number) {
                    return values.stream()
                            .mapToDouble(v -> ((Number) v).doubleValue())
                            .average()
                            .orElse(0.0);
                }
                throw new IllegalArgumentException("La fonction AVG ne peut être appliquée qu'à des nombres");
                
            case "min":
                if (values.get(0) instanceof Number) {
                    return values.stream()
                            .mapToDouble(v -> ((Number) v).doubleValue())
                            .min()
                            .orElse(0.0);
                } else if (values.get(0) instanceof String) {
                    return values.stream()
                            .map(Object::toString)
                            .min(String::compareTo)
                            .orElse("");
                }
                throw new IllegalArgumentException("Type non supporté pour la fonction MIN");
                
            case "max":
                if (values.get(0) instanceof Number) {
                    return values.stream()
                            .mapToDouble(v -> ((Number) v).doubleValue())
                            .max()
                            .orElse(0.0);
                } else if (values.get(0) instanceof String) {
                    return values.stream()
                            .map(Object::toString)
                            .max(String::compareTo)
                            .orElse("");
                }
                throw new IllegalArgumentException("Type non supporté pour la fonction MAX");
                
            default:
                throw new IllegalArgumentException("Fonction d'agrégation inconnue: " + function);
        }
    }
    
    /**
     * Effectue un regroupement (GROUP BY) et applique une fonction d'agrégation
     * @param tableName Nom de la table
     * @param groupByColumn Colonne pour le regroupement
     * @param aggregateColumn Colonne pour l'agrégation
     * @param function Fonction d'agrégation (sum, avg, min, max, count)
     * @return Table résultat avec les groupes et les valeurs agrégées
     */
    public Table groupBy(String tableName, String groupByColumn, String aggregateColumn, String function) {
        Table table = getTable(tableName);
        
        if (!table.getColumnNames().contains(groupByColumn)) {
            throw new IllegalArgumentException("Colonne de regroupement inconnue: " + groupByColumn);
        }
        
        if (!table.getColumnNames().contains(aggregateColumn)) {
            throw new IllegalArgumentException("Colonne d'agrégation inconnue: " + aggregateColumn);
        }
        
        // Créer des définitions de colonnes pour la table résultante
        List<ColumnDefinition> groupColumnDefs = new ArrayList<>();
        
        // Trouver le type de la colonne de regroupement
        String groupColType = table.getColumnDefinitions().stream()
            .filter(def -> def.getName().equals(groupByColumn))
            .findFirst()
            .orElseThrow(() -> new IllegalArgumentException("Colonne introuvable: " + groupByColumn))
            .getType();
            
        // Ajouter la colonne de groupe
        groupColumnDefs.add(new ColumnDefinition(groupByColumn, groupColType, true, true));
        
        // Ajouter la colonne de résultat d'agrégation
        groupColumnDefs.add(new ColumnDefinition(
            function + "(" + aggregateColumn + ")", 
            "DOUBLE",
            true,
            false
        ));
        
        // Créer la table résultat
        Table result = new Table(tableName + "_grouped", groupColumnDefs);
        
        // Regrouper les valeurs
        Map<Object, List<Object>> groups = new HashMap<>();
        
        for (int i = 0; i < table.getRowCount(); i++) {
            Map<String, Object> row = table.getRow(i);
            Object groupValue = row.get(groupByColumn);
            Object aggregateValue = row.get(aggregateColumn);
            
            if (aggregateValue != null) {
                groups.computeIfAbsent(groupValue, k -> new ArrayList<>()).add(aggregateValue);
            }
        }
        
        // Calculer les agrégations pour chaque groupe
        for (Map.Entry<Object, List<Object>> entry : groups.entrySet()) {
            Object groupValue = entry.getKey();
            List<Object> values = entry.getValue();
            
            if (values.isEmpty()) {
                continue;
            }
            
            Object aggregateResult = computeAggregation(values, function);
            
            Map<String, Object> resultRow = new HashMap<>();
            resultRow.put(groupByColumn, groupValue);
            resultRow.put(function + "(" + aggregateColumn + ")", aggregateResult);
            
            result.addRow(resultRow);
        }
        
        return result;
    }
    
    /**
     * Calcule une agrégation sur une liste de valeurs
     */
    private Object computeAggregation(List<Object> values, String function) {
        if (values.isEmpty()) {
            return null;
        }
        
        switch (function.toLowerCase()) {
            case "count":
                return values.size();
                
            case "sum":
                if (values.get(0) instanceof Number) {
                    return values.stream()
                            .mapToDouble(v -> ((Number) v).doubleValue())
                            .sum();
                }
                throw new IllegalArgumentException("La fonction SUM ne peut être appliquée qu'à des nombres");
                
            case "avg":
                if (values.get(0) instanceof Number) {
                    return values.stream()
                            .mapToDouble(v -> ((Number) v).doubleValue())
                            .average()
                            .orElse(0.0);
                }
                throw new IllegalArgumentException("La fonction AVG ne peut être appliquée qu'à des nombres");
                
            case "min":
                if (values.get(0) instanceof Number) {
                    return values.stream()
                            .mapToDouble(v -> ((Number) v).doubleValue())
                            .min()
                            .orElse(0.0);
                } else if (values.get(0) instanceof String) {
                    return values.stream()
                            .map(Object::toString)
                            .min(String::compareTo)
                            .orElse("");
                }
                throw new IllegalArgumentException("Type non supporté pour la fonction MIN");
                
            case "max":
                if (values.get(0) instanceof Number) {
                    return values.stream()
                            .mapToDouble(v -> ((Number) v).doubleValue())
                            .max()
                            .orElse(0.0);
                } else if (values.get(0) instanceof String) {
                    return values.stream()
                            .map(Object::toString)
                            .max(String::compareTo)
                            .orElse("");
                }
                throw new IllegalArgumentException("Type non supporté pour la fonction MAX");
                
            default:
                throw new IllegalArgumentException("Fonction d'agrégation inconnue: " + function);
        }
    }
} 
 