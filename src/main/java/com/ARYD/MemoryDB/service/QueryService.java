package com.ARYD.MemoryDB.service;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
public class QueryService {

    // Supposons que TableService permet de récupérer un DataFrame par son nom
    private final TablesService tablesService;

    /**
     * Exécute une requête SQL simple de type :
     * SELECT column1, column2 FROM tableName [WHERE columnX eq "value"]
     * ou une agrégation de type :
     * SELECT SUM(nomColonne) FROM tableName [WHERE ...]
     * SELECT COUNT(nomColonne) FROM tableName [WHERE ...]
     * (COUNT(*) est supporté en passant "*" en argument)
     */
    public List<Map<String, Object>> executeQuery(String query) {
        // Normaliser la requête (supprimer les espaces inutiles, etc.)
        query = query.trim();

        // Vérifier que la requête commence par SELECT
        if (!query.toUpperCase().startsWith("SELECT ")) {
            throw new IllegalArgumentException("La requête doit commencer par SELECT");
        }

        int fromIndex = query.toUpperCase().indexOf(" FROM ");
        if (fromIndex == -1) {
            throw new IllegalArgumentException("La requête doit contenir FROM");
        }

        // Extraction de la partie SELECT
        String selectPart = query.substring(7, fromIndex).trim();

        // Détecter les agrégations SUM et COUNT
        boolean isSumQuery = false;
        String sumColumn = "";
        boolean isCountQuery = false;
        String countColumn = "";
        String selectPartUpper = selectPart.toUpperCase();
        if (selectPartUpper.startsWith("SUM(") && selectPart.endsWith(")")) {
            isSumQuery = true;
            sumColumn = selectPart.substring(4, selectPart.length() - 1).trim();
        } else if (selectPartUpper.startsWith("COUNT(") && selectPart.endsWith(")")) {
            isCountQuery = true;
            countColumn = selectPart.substring(6, selectPart.length() - 1).trim();
        }

        List<String> columnsToSelect;
        // Si SELECT * on prend toutes les colonnes
        if (selectPart.equals("*")) {
            columnsToSelect = null; // On verra plus tard pour définir cette liste via le DataFrame
        } else {
            columnsToSelect = Arrays.stream(selectPart.split(","))
                    .map(String::trim)
                    .collect(Collectors.toList());
        }

        // Extraction de la partie FROM
        String afterFrom = query.substring(fromIndex + 6).trim();
        String tableName;
        String whereClause = null;
        int whereIndex = afterFrom.toUpperCase().indexOf(" WHERE ");
        if (whereIndex != -1) {
            tableName = afterFrom.substring(0, whereIndex).trim();
            whereClause = afterFrom.substring(whereIndex + 7).trim();
        } else {
            tableName = afterFrom.trim();
        }

        // Récupérer le DataFrame via TableService
        DataFrameService df = tablesService.getTableByName(tableName);
        if (df == null) {
            throw new IllegalArgumentException("Table " + tableName + " introuvable.");
        }

        // Filtrer les lignes si une clause WHERE est présente
        List<Map<String, Object>> rows;
        if (whereClause != null && !whereClause.isEmpty()) {
            // On suppose ici que la clause WHERE est du type: column eq "value"
            String[] tokens = whereClause.split(" ");
            if (tokens.length < 3) {
                throw new IllegalArgumentException("Clause WHERE invalide.");
            }
            String whereColumn = tokens[0].trim();
            String operator = tokens[1].trim();
            String valueToken = whereClause.substring(whereClause.indexOf(operator) + operator.length()).trim();
            // Retirer les guillemets éventuels
            String whereValue = valueToken.replaceAll("^\"|\"$", "");

            // Filtrer le DataFrame
            DataFrameService filteredDf = df.filter(whereColumn, whereValue);
            rows = new ArrayList<>();
            for (int i = 0; i < filteredDf.countRows(); i++) {
                rows.add(filteredDf.getRow(i));
            }
        } else {
            // Pas de clause WHERE : récupérer toutes les lignes
            rows = new ArrayList<>();
            for (int i = 0; i < df.countRows(); i++) {
                rows.add(df.getRow(i));
            }
        }

        // Si c'est une requête SUM, calculer l'agrégation et retourner le résultat
        if (isSumQuery) {
            double sum = 0.0;
            for (Map<String, Object> row : rows) {
                Object value = row.get(sumColumn);
                if (value instanceof Number) {
                    sum += ((Number) value).doubleValue();
                } else if (value != null) {
                    try {
                        sum += Double.parseDouble(value.toString());
                    } catch (NumberFormatException e) {
                        // Ignorer la valeur si elle n'est pas convertible en nombre
                    }
                }
            }
            Map<String, Object> sumResult = new LinkedHashMap<>();
            sumResult.put("SUM(" + sumColumn + ")", sum);
            return Collections.singletonList(sumResult);
        }

        // Si c'est une requête COUNT, calculer le décompte et retourner le résultat
        if (isCountQuery) {
            int count = 0;
            if (countColumn.equals("*")) {
                count = rows.size();
            } else {
                for (Map<String, Object> row : rows) {
                    if (row.get(countColumn) != null) {
                        count++;
                    }
                }
            }
            Map<String, Object> countResult = new LinkedHashMap<>();
            countResult.put("COUNT(" + countColumn + ")", count);
            return Collections.singletonList(countResult);
        }

        // Si SELECT * est demandé, on récupère la liste de toutes les colonnes
        if (columnsToSelect == null) {
            columnsToSelect = new ArrayList<>(df.getColumns().keySet());
        }

        // Appliquer la projection : conserver uniquement les colonnes demandées
        List<String> finalColumnsToSelect = columnsToSelect;
        List<Map<String, Object>> result = rows.stream().map(row -> {
            Map<String, Object> projectedRow = new LinkedHashMap<>();
            for (String col : finalColumnsToSelect) {
                // On ajoute la colonne si elle existe, sinon on met null
                projectedRow.put(col, row.getOrDefault(col, null));
            }
            return projectedRow;
        }).collect(Collectors.toList());

        return result;
    }
}