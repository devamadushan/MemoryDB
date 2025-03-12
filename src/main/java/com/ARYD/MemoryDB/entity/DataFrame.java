package com.ARYD.MemoryDB.entity;

import lombok.Getter;
import lombok.Setter;

import java.util.*;

public class DataFrame {
    @Getter
    private Map<String, List<Map<String,List<Object>>>> columns = new LinkedHashMap<>();
    private int rowCount = 0; // Nombre total de lignes

    @Getter @Setter
    private String tableName;

    // Ajouter une colonne vide
    public void addColumn(String name) {
        columns.put(name, new ArrayList<>(Collections.nCopies(rowCount, null)));
    }

    // Ajouter une colonne avec des valeurs
    public void addColumn(String name, List<Object> values) {
        if (rowCount != 0 && values.size() != rowCount) {
            throw new IllegalArgumentException("La colonne doit avoir " + rowCount + " valeurs !");
        }
        columns.put(name, new ArrayList<>(values));
        rowCount = values.size();
    }

    // Ajouter une ligne complète
    public void addRow(Map<String, Object> row) {
        for (String col : columns.keySet()) {
            columns.get(col).add(row.getOrDefault(col, null)); // Ajoute null si la colonne est absente
        }
        rowCount++;
    }

    // Récupérer une colonne complète
    public List<Object> getColumn(String name) {
        return columns.get(name);
    }

    // Récupérer une ligne complète
    public Map<String, Object> getRow(int index) {
        if (index < 0 || index >= rowCount) {
            throw new IndexOutOfBoundsException("Index hors limites !");
        }
        Map<String, Object> row = new LinkedHashMap<>();
        for (Map.Entry<String, List<Object>> entry : columns.entrySet()) {
            row.put(entry.getKey(), entry.getValue().get(index));
        }
        return row;
    }

    // Supprimer une colonne
    public void removeColumn(String name) {
        columns.remove(name);
    }

    // Supprimer une ligne
    public void removeRow(int index) {
        if (index < 0 || index >= rowCount) {
            throw new IndexOutOfBoundsException("Index hors limites !");
        }
        for (List<Object> values : columns.values()) {
            values.remove(index);
        }
        rowCount--;
    }

    // Filtrer les lignes sur une colonne donnée
    public DataFrame filter(String columnName, Object value) {
        DataFrame filtered = new DataFrame();
        for (String col : columns.keySet()) {
            filtered.addColumn(col);
        }
        List<Object> targetColumn = columns.get(columnName);
        for (int i = 0; i < rowCount; i++) {
            if (Objects.equals(targetColumn.get(i), value)) {
                filtered.addRow(getRow(i));
            }
        }
        return filtered;
    }

    // Afficher le DataFrame
    public void print() {
        System.out.println(columns.keySet());
        for (int i = 0; i < rowCount; i++) {
            System.out.println(getRow(i).values());
        }
    }
}