package com.ARYD.MemoryDB.entity;

import lombok.Getter;
import lombok.Setter;

import java.io.StringWriter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class DataFrame {
    @Getter
    private Map<String, Map<String, List<Object>>> columns = new LinkedHashMap<>();

    // Utiliser AtomicInteger pour être thread-safe
    private AtomicInteger rowCount = new AtomicInteger(0);

    @Getter @Setter
    private String tableName;

    // Ajouter une colonne vide avec allocation initiale
    public void addColumn(String name) {
        Map<String, List<Object>> columnData = new LinkedHashMap<>();
        // Pré-allocation avec capacité initiale
        columnData.put("values", new ArrayList<>(Math.max(1000, rowCount.get())));
        columns.put(name, columnData);
    }

    // Ajouter une colonne avec des valeurs
    public void addColumn(String name, List<Object> values) {
        int currentRows = rowCount.get();
        if (currentRows != 0 && values.size() != currentRows) {
            throw new IllegalArgumentException("La colonne doit avoir " + currentRows + " valeurs !");
        }
        Map<String, List<Object>> columnData = new LinkedHashMap<>();
        columnData.put("values", new ArrayList<>(values));
        columns.put(name, columnData);
        rowCount.set(values.size());
    }

    // Ajouter une ligne complète
    public synchronized void addRow(Map<String, Object> row) {
        for (String col : columns.keySet()) {
            Map<String, List<Object>> columnData = columns.get(col);
            columnData.get("values").add(row.getOrDefault(col, null));
        }
        rowCount.incrementAndGet();
    }

    // Ajouter des lignes en batch (plus efficace)
    public synchronized void addRows(List<Map<String, Object>> rows) {
        if (rows.isEmpty()) return;

        // Optimisation: traiter colonne par colonne plutôt que ligne par ligne
        for (String col : columns.keySet()) {
            List<Object> columnValues = columns.get(col).get("values");
            for (Map<String, Object> row : rows) {
                columnValues.add(row.getOrDefault(col, null));
            }
        }
        rowCount.addAndGet(rows.size());
    }

    // Récupérer une colonne complète
    public List<Object> getColumn(String name) {
        return columns.get(name).get("values");
    }

    public String toCSV() {
        StringWriter writer = new StringWriter();

        // Écrire l'en-tête CSV
        writer.append(String.join(",", columns.keySet())).append("\n");

        // Écrire les lignes
        int rows = rowCount.get();
        for (int i = 0; i < rows; i++) {
            Map<String, Object> row = getRow(i);
            String line = row.values().stream()
                    .map(value -> value == null ? "" : value.toString())
                    .collect(Collectors.joining(","));
            writer.append(line).append("\n");
        }

        return writer.toString();
    }

    public void printRowCount() {
        System.out.println("Nombre de lignes : " + countRows());
    }

    // Récupérer une ligne complète
    public Map<String, Object> getRow(int index) {
        int rows = rowCount.get();
        if (index < 0 || index >= rows) {
            throw new IndexOutOfBoundsException("Index hors limites !");
        }
        Map<String, Object> row = new LinkedHashMap<>();
        for (Map.Entry<String, Map<String, List<Object>>> entry : columns.entrySet()) {
            row.put(entry.getKey(), entry.getValue().get("values").get(index));
        }
        return row;
    }

    // Afficher le DataFrame au format CSV
    public void printAsCSV() {
        // Construction de l'en-tête
        String header = String.join(",", columns.keySet());
        System.out.println(header);

        // Construction et affichage des lignes
        int rows = rowCount.get();
        for (int i = 0; i < rows; i++) {
            Map<String, Object> row = getRow(i);
            String line = row.values().stream()
                    .map(value -> value == null ? "" : value.toString())
                    .collect(Collectors.joining(","));
            System.out.println(line);
        }
    }

    // Afficher uniquement les lignes où une colonne a une certaine valeur
    public void printRowsByColumnValue(String columnName, Object value) {
        // Vérifier que la colonne existe
        if (!columns.containsKey(columnName)) {
            System.out.println("La colonne " + columnName + " n'existe pas.");
            return;
        }

        // Afficher l'en-tête CSV
        System.out.println(String.join(",", columns.keySet()));

        // Parcourir les lignes et afficher celles qui correspondent
        int rows = rowCount.get();
        for (int i = 0; i < rows; i++) {
            Map<String, Object> row = getRow(i);
            if (Objects.equals(row.get(columnName), value)) {
                String line = row.values().stream()
                        .map(v -> v == null ? "" : v.toString())
                        .collect(Collectors.joining(","));
                System.out.println(line);
            }
        }
    }

    public DataFrame filter(String columnName, Object value) {
        DataFrame filteredDf = new DataFrame();
        filteredDf.setTableName(this.tableName); // On garde le même nom de table

        // Vérifier que la colonne existe
        if (!columns.containsKey(columnName)) {
            throw new IllegalArgumentException("La colonne '" + columnName + "' n'existe pas.");
        }

        // Initialiser les colonnes dans le DataFrame filtré
        for (String col : columns.keySet()) {
            filteredDf.addColumn(col);
        }

        // Optimisation: récupérer directement la liste des valeurs de la colonne de filtrage
        List<Object> columnValues = columns.get(columnName).get("values");
        int rows = rowCount.get();

        // Traitement par lots pour plus d'efficacité
        List<Map<String, Object>> batchRows = new ArrayList<>(1000);

        for (int i = 0; i < rows; i++) {
            Object cellValue = columnValues.get(i);
            boolean match = false;

            // Si la valeur passée est une chaîne et que la valeur dans la table est un nombre
            if (cellValue instanceof Number && value instanceof String) {
                try {
                    long queryValue = Long.parseLong((String) value);
                    match = (((Number) cellValue).longValue() == queryValue);
                } catch (NumberFormatException e) {
                    // Conversion impossible, match reste false
                }
            } else {
                // Optimisation: éviter la conversion en String pour les comparaisons d'égalité quand possible
                match = value == null ? cellValue == null : value.equals(cellValue);

                // Si échec, on essaie la comparaison de chaînes
                if (!match) {
                    match = String.valueOf(cellValue).equals(String.valueOf(value));
                }
            }

            if (match) {
                batchRows.add(getRow(i));

                // Quand on atteint la taille du lot, on l'ajoute au DataFrame filtré
                if (batchRows.size() >= 1000) {
                    filteredDf.addRows(batchRows);
                    batchRows.clear();
                }
            }
        }

        // Ajouter le reste des lignes
        if (!batchRows.isEmpty()) {
            filteredDf.addRows(batchRows);
        }

        return filteredDf;
    }

    public int countRows() {
        return rowCount.get();
    }

    // Méthode pour redimensionner toutes les colonnes à une taille fixe
    // Utile après un chargement parallèle pour s'assurer que toutes les colonnes ont la même taille
    public synchronized void normalizeColumnsSize() {
        int targetSize = rowCount.get();
        for (Map<String, List<Object>> columnData : columns.values()) {
            List<Object> values = columnData.get("values");
            int currentSize = values.size();

            if (currentSize < targetSize) {
                // Ajouter des valeurs null pour compléter
                for (int i = currentSize; i < targetSize; i++) {
                    values.add(null);
                }
            } else if (currentSize > targetSize) {
                // Tronquer les valeurs en excès
                while (values.size() > targetSize) {
                    values.remove(values.size() - 1);
                }
            }
        }
    }
}