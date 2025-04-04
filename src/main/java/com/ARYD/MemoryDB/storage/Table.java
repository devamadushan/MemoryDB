package com.ARYD.MemoryDB.storage;

import com.ARYD.MemoryDB.types.DataType;
import com.ARYD.MemoryDB.types.DataTypeFactory;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Table optimisée pour le stockage en mémoire avec organisation colonnaire
 */
@Slf4j
public class Table {
    
    private final String name;                                      // Nom de la table
    private final Map<String, ColumnStorage> columns;               // Colonnes de la table
    private final List<String> columnOrder;                         // Ordre des colonnes
    private final Map<String, Map<Object, List<Integer>>> indexes;  // Index
    private final AtomicInteger rowCount;                           // Nombre de lignes
    private final List<ColumnDefinition> columnDefinitions;
    
    /**
     * Constructeur
     * @param name Nom de la table
     * @param columnDefinitions Définitions des colonnes
     */
    public Table(String name, List<ColumnDefinition> columnDefinitions) {
        this.name = name;
        this.columnDefinitions = columnDefinitions;
        this.columns = new ConcurrentHashMap<>();
        this.columnOrder = new ArrayList<>();
        this.indexes = new ConcurrentHashMap<>();
        this.rowCount = new AtomicInteger(0);

        // Initialiser les colonnes
        for (ColumnDefinition columnDef : columnDefinitions) {
            DataType dataType = DataTypeFactory.getType(columnDef.getType());
            columns.put(columnDef.getName(), new ColumnStorage(columnDef.getName(), dataType));
            columnOrder.add(columnDef.getName());
        }
    }
    
    /**
     * Ajoute une colonne à la table
     * @param name Nom de la colonne
     * @param typeName Nom du type de données
     * @param indexed Indique si la colonne doit être indexée
     */
    public void addColumn(String name, String typeName, boolean indexed) {
        if (columns.containsKey(name)) {
            throw new IllegalArgumentException("La colonne '" + name + "' existe déjà");
        }
        
        // Créer une instance de type à partir du nom
        DataType dataType = DataTypeFactory.getType(typeName);
        
        // Créer le stockage de la colonne
        ColumnStorage columnStorage = new ColumnStorage(name, dataType);
        
        // Ajouter la colonne
        columns.put(name, columnStorage);
        columnOrder.add(name);
        
        // Créer l'index si nécessaire
        if (indexed) {
            indexes.put(name, new HashMap<>());
        }
    }
    
    /**
     * Ajoute une ligne à la table (avec des chaînes)
     * @param values Valeurs de la ligne (ordre des colonnes)
     * @return Index de la ligne ajoutée
     */
    public int addRowFromStrings(Map<String, String> values) {
        // Vérifier si toutes les colonnes sont présentes
        for (String column : columnOrder) {
            if (!values.containsKey(column)) {
                throw new IllegalArgumentException("Valeur manquante pour la colonne '" + column + "'");
            }
        }
        
        // Ajouter les valeurs colonne par colonne
        int rowIndex = rowCount.getAndIncrement();
        
        for (String column : columnOrder) {
            String value = values.get(column);
            ColumnStorage columnStorage = columns.get(column);
            
            // Ajouter la valeur à la colonne
            columnStorage.addValue(value);
            
            // Mettre à jour l'index si nécessaire
            if (indexes.containsKey(column)) {
                Object indexValue = columnStorage.getValue(rowIndex);
                indexes.get(column)
                       .computeIfAbsent(indexValue, k -> new ArrayList<>())
                       .add(rowIndex);
            }
        }
        
        return rowIndex;
    }
    
    /**
     * Récupère une ligne par son index
     * @param rowIndex Index de la ligne
     * @return Valeurs de la ligne
     */
    public Map<String, Object> getRow(int rowIndex) {
        if (rowIndex < 0 || rowIndex >= rowCount.get()) {
            throw new IndexOutOfBoundsException("Index de ligne invalide: " + rowIndex);
        }
        
        Map<String, Object> row = new LinkedHashMap<>();
        
        for (String column : columnOrder) {
            ColumnStorage columnStorage = columns.get(column);
            row.put(column, columnStorage.getValue(rowIndex));
        }
        
        return row;
    }
    
    /**
     * Filtre les lignes selon une condition
     * @param columnName Nom de la colonne
     * @param value Valeur recherchée
     * @return Table filtrée
     */
    public Table filter(String columnName, Object value) {
        if (!columns.containsKey(columnName)) {
            throw new IllegalArgumentException("Colonne inconnue: " + columnName);
        }
        
        // Créer une nouvelle table pour le résultat
        Table result = new Table(this.name + "_filtered", columnDefinitions);
        
        // Copier la structure (colonnes)
        for (String column : columnOrder) {
            ColumnStorage storage = columns.get(column);
            result.addColumn(column, storage.getDataType().getClass().getSimpleName(), false);
        }
        
        // Si on a un index sur cette colonne, l'utiliser
        if (indexes.containsKey(columnName)) {
            List<Integer> matchingRows = indexes.get(columnName).getOrDefault(value, Collections.emptyList());
            
            for (int rowIndex : matchingRows) {
                Map<String, Object> row = getRow(rowIndex);
                // Transformer la ligne pour la nouvelle table
                result.addRow(row);
            }
        } else {
            // Sinon, faire un parcours séquentiel
            ColumnStorage columnStorage = columns.get(columnName);
            
            for (int i = 0; i < rowCount.get(); i++) {
                Object cellValue = columnStorage.getValue(i);
                
                if ((cellValue == null && value == null) || 
                    (cellValue != null && cellValue.equals(value))) {
                    Map<String, Object> row = getRow(i);
                    result.addRow(row);
                }
            }
        }
        
        return result;
    }
    
    /**
     * Retourne le nombre de lignes dans la table
     */
    public int getRowCount() {
        return rowCount.get();
    }
    
    /**
     * Retourne le nombre de colonnes dans la table
     */
    public int getColumnCount() {
        return columnOrder.size();
    }
    
    /**
     * Retourne le nom de la table
     */
    public String getName() {
        return name;
    }
    
    /**
     * Retourne la liste des noms de colonnes
     */
    public List<String> getColumnNames() {
        return new ArrayList<>(columnOrder);
    }
    
    /**
     * Retourne la liste des types de colonnes
     */
    public Map<String, DataType> getColumnTypes() {
        Map<String, DataType> types = new LinkedHashMap<>();
        
        for (String column : columnOrder) {
            types.put(column, columns.get(column).getDataType());
        }
        
        return types;
    }
    
    /**
     * Crée un index sur une colonne
     * @param columnName Nom de la colonne
     */
    public void createIndex(String columnName) {
        if (!columns.containsKey(columnName)) {
            throw new IllegalArgumentException("Colonne inconnue: " + columnName);
        }
        
        if (indexes.containsKey(columnName)) {
            // L'index existe déjà
            return;
        }
        
        // Créer l'index
        Map<Object, List<Integer>> index = new HashMap<>();
        indexes.put(columnName, index);
        
        // Remplir l'index
        ColumnStorage columnStorage = columns.get(columnName);
        
        for (int i = 0; i < rowCount.get(); i++) {
            Object value = columnStorage.getValue(i);
            index.computeIfAbsent(value, k -> new ArrayList<>()).add(i);
        }
    }
    
    /**
     * Supprime un index sur une colonne
     * @param columnName Nom de la colonne
     */
    public void dropIndex(String columnName) {
        indexes.remove(columnName);
    }
    
    /**
     * Convertit la table en CSV
     */
    public String toCSV() {
        StringBuilder sb = new StringBuilder();
        
        // En-tête
        sb.append(String.join(",", columnOrder)).append("\n");
        
        // Lignes
        for (int i = 0; i < rowCount.get(); i++) {
            Map<String, Object> row = getRow(i);
            
            boolean first = true;
            for (String column : columnOrder) {
                if (!first) {
                    sb.append(",");
                }
                
                Object value = row.get(column);
                if (value != null) {
                    sb.append(value);
                }
                
                first = false;
            }
            
            sb.append("\n");
        }
        
        return sb.toString();
    }
    
    /**
     * Retourne une estimation de la mémoire utilisée par la table
     */
    public long estimateMemoryUsage() {
        long total = 0;
        
        // Mémoire pour les colonnes
        for (ColumnStorage column : columns.values()) {
            DataType type = column.getDataType();
            
            if (type.isFixedSize()) {
                // Pour les types à taille fixe, c'est simple
                total += (long) column.size() * type.getSizeInBytes();
            } else {
                // Pour les types à taille variable, c'est une estimation
                total += column.size() * 8; // Estimation
            }
        }
        
        // Mémoire pour les index
        for (Map<Object, List<Integer>> index : indexes.values()) {
            // Taille approximative: nombre d'entrées * (taille clé + 4 octets par référence)
            total += index.size() * 12;
            
            // Ajouter la taille des listes d'indices
            for (List<Integer> indices : index.values()) {
                total += indices.size() * 4;
            }
        }
        
        return total;
    }

    /**
     * Ajoute une valeur à une colonne spécifique
     * @param columnName Nom de la colonne
     * @param value Valeur à ajouter
     */
    public void addValue(String columnName, Object value) {
        ColumnStorage column = columns.get(columnName);
        if (column != null) {
            column.addValue(value);
            // Incrémente le compteur de lignes seulement pour la première colonne
            if (columnName.equals(columnDefinitions.get(0).getName())) {
                rowCount.incrementAndGet();
            }
        } else {
            log.warn("Colonne non trouvée: {}", columnName);
        }
    }

    /**
     * Ajoute une ligne complète (avec des objets)
     * @param rowData Données de la ligne (nom de colonne -> valeur)
     */
    public void addRow(Map<String, Object> rowData) {
        // Pour éviter les conflits de signature, on utilise cette méthode uniquement 
        // pour ajouter sans retourner d'index
        for (Map.Entry<String, Object> entry : rowData.entrySet()) {
            addValue(entry.getKey(), entry.getValue());
        }
    }

    /**
     * Récupère une valeur spécifique
     * @param rowIndex Index de la ligne
     * @param columnName Nom de la colonne
     * @return Valeur à l'intersection
     */
    public Object getValue(int rowIndex, String columnName) {
        ColumnStorage column = columns.get(columnName);
        return column != null ? column.getValue(rowIndex) : null;
    }

    /**
     * Retourne les définitions des colonnes
     */
    public List<ColumnDefinition> getColumnDefinitions() {
        return columnDefinitions;
    }

    /**
     * Retourne les colonnes
     */
    public Map<String, ColumnStorage> getColumns() {
        return columns;
    }
} 
 