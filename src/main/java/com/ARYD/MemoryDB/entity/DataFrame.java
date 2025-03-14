package com.ARYD.MemoryDB.entity;

import lombok.Getter;
import lombok.Setter;

import java.io.StringWriter;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

public class DataFrame {
    @Getter
    private Map<String, Map<String, List<Object>>> columns = new LinkedHashMap<>();
    private int rowCount = 0; // Nombre total de lignes

    @Getter @Setter
    private String tableName;

    // Ajouter une colonne vide
    public void addColumn(String name) {
        Map<String, List<Object>> columnData = new LinkedHashMap<>();
        columnData.put("values", new ArrayList<>(Collections.nCopies(rowCount, null)));
        columns.put(name, columnData);
    }

    // Ajouter une colonne avec des valeurs
    public void addColumn(String name, List<Object> values) {
        if (rowCount != 0 && values.size() != rowCount) {
            throw new IllegalArgumentException("La colonne doit avoir " + rowCount + " valeurs !");
        }
        Map<String, List<Object>> columnData = new LinkedHashMap<>();
        columnData.put("values", new ArrayList<>(values));
        columns.put(name, columnData);
        rowCount = values.size();
    }

    // Ajouter une ligne complète
    public void addRow(Map<String, Object> row) {
        for (String col : columns.keySet()) {
            Map<String, List<Object>> columnData = columns.get(col);
            columnData.get("values").add(row.getOrDefault(col, null)); // Ajoute null si absent
        }
        rowCount++;
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
        for (int i = 0; i < rowCount; i++) {
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
        if (index < 0 || index >= rowCount) {
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
        //System.out.println(header);

        // Construction et affichage des lignes
        for (int i = 0; i < rowCount; i++) {
            Map<String, Object> row = getRow(i);
            String line = row.values().stream()
                    .map(value -> value == null ? "" : value.toString())
                    .collect(Collectors.joining(","));
            //System.out.println(line);
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
        for (int i = 0; i < rowCount; i++) {
            Map<String, Object> row = getRow(i);
            if (Objects.equals(row.get(columnName), value)) {
                String line = row.values().stream()
                        .map(v -> v == null ? "" : v.toString())
                        .collect(Collectors.joining(","));
                //System.out.println(line);
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

        // On parcourt toutes les lignes pour trouver celles qui correspondent
        for (int i = 0; i < countRows(); i++) {
            Object cellValue = columns.get(columnName).get("values").get(i);
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
                // Sinon, on compare sous forme de chaînes pour être sûr
                match = String.valueOf(cellValue).equals(String.valueOf(value));
            }

            if (match) {
                filteredDf.addRow(getRow(i));
            }
        }
        return filteredDf;
    }
    public DataFrame filterContains(String columnName, String value) {
        // Vérification
        if (!this.columns.containsKey(columnName)) {
            throw new IllegalArgumentException("La colonne '" + columnName + "' n'existe pas dans le DataFrame.");
        }

        // Créer un nouveau DataFrame pour stocker le résultat du filtrage
        DataFrame filteredDf = new DataFrame();
        filteredDf.setTableName(this.tableName); // Conserver le nom de la table

        // Copier toutes les colonnes du DataFrame d'origine
        for (String col : this.columns.keySet()) {
            filteredDf.addColumn(col);
        }

        // Parcourir chaque ligne pour vérifier la condition
        for (int i = 0; i < this.countRows(); i++) {
            Object cellValue = this.getRow(i).get(columnName);

            // Convertir toutes les valeurs en String avant de vérifier contains()
            String cellString = (cellValue == null) ? "" : cellValue.toString();

            // Vérifier si la valeur contient le mot-clé recherché
            if (cellString.contains(value)) {
                filteredDf.addRow(this.getRow(i)); // Ajouter la ligne si la condition est remplie
            }
        }

        return filteredDf;
    }

    public DataFrame filterYear(String columnName, int targetYear) {
        // Vérifier si la colonne existe dans le DataFrame
        if (!columns.containsKey(columnName)) {
            throw new IllegalArgumentException("La colonne '" + columnName + "' n'existe pas.");
        }

        // Créer un nouveau DataFrame pour stocker les résultats filtrés
        DataFrame filteredDf = new DataFrame();
        filteredDf.setTableName(this.tableName); // Garder le même nom de table

        // Copier la structure des colonnes depuis le DataFrame original
        for (String col : columns.keySet()) {
            filteredDf.addColumn(col);
        }

        // Définir un format de date standard (YYYY-MM-DD)
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");

        // Parcourir toutes les lignes pour extraire l'année et filtrer les résultats
        for (int i = 0; i < countRows(); i++) {
            Object cellValue = getRow(i).get(columnName);
            int year = -1; // Valeur par défaut

            try {
                if (cellValue instanceof Date) {
                    // Si la valeur est de type Date, extraire l'année avec Calendar
                    Calendar cal = Calendar.getInstance();
                    cal.setTime((Date) cellValue);
                    year = cal.get(Calendar.YEAR);
                } else if (cellValue instanceof String) {
                    // Si la valeur est une chaîne, essayer de la convertir en Date
                    Date date = dateFormat.parse((String) cellValue);
                    Calendar cal = Calendar.getInstance();
                    cal.setTime(date);
                    year = cal.get(Calendar.YEAR);
                }
            } catch (ParseException ignored) {
                // En cas d'erreur de conversion, ignorer cette ligne
            }

            // Ajouter la ligne au DataFrame filtré si l'année correspond à `targetYear`
            if (year == targetYear) {
                filteredDf.addRow(getRow(i));
            }
        }

        return filteredDf;
    }


    private List<Map<String, Object>> getAllRows(DataFrame df) {
        List<Map<String, Object>> rows = new ArrayList<>();
        for (int i = 0; i < df.countRows(); i++) {
            rows.add(df.getRow(i));
        }
        return rows;
    }





    public int countRows() {
        return rowCount;
    }
}