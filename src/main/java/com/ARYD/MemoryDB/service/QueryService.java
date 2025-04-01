package com.ARYD.MemoryDB.service;

import com.ARYD.MemoryDB.entity.DataFrame;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
public class QueryService {

    private final DataFrameService dataFrameService;

    // Méthode publique unique qui retourne Object (peut être List<Map<String, Object>> ou Map<String, Object>)
    public Object executeQuery(String query) {
        query = query.trim();
        // Détection simple d'agrégats dans la requête (MIN, MAX, SUM, AVG, COUNT)
        if (query.matches("(?i).*\\b(MIN|MAX|SUM|AVG|COUNT)\\s*\\(.*\\).*")) {
            return executeAggregateQuery(query);
        } else {
            return executeSelectQuery(query);
        }
    }

    // Exécution d'un SELECT classique sans agrégats
    private List<Map<String, Object>> executeSelectQuery(String query) {
        // On commence par vérifier que la requête commence par SELECT
        if (!query.toUpperCase().startsWith("SELECT ")) {
            throw new IllegalArgumentException("La requête doit commencer par SELECT");
        }

        int fromIndex = query.toUpperCase().indexOf(" FROM ");
        if (fromIndex == -1) {
            throw new IllegalArgumentException("La requête doit contenir FROM");
        }

        String selectPart = query.substring(7, fromIndex).trim();
        List<String> columnsToSelect;
        if (selectPart.equals("*")) {
            columnsToSelect = null;
        } else {
            columnsToSelect = Arrays.stream(selectPart.split(","))
                    .map(String::trim)
                    .collect(Collectors.toList());
        }

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

        DataFrame df = dataFrameService.getTableByName(tableName);
        if (df == null) {
            throw new IllegalArgumentException("Table " + tableName + " introuvable.");
        }

        // Appliquer la clause WHERE si présente
        List<Map<String, Object>> rows;
        if (whereClause != null && !whereClause.isEmpty()) {
            // Exemple simple : "column eq \"value\""
            String[] tokens = whereClause.split(" ");
            if (tokens.length < 3) {
                throw new IllegalArgumentException("Clause WHERE invalide.");
            }
            String whereColumn = tokens[0].trim();
            String operator = tokens[1].trim();
            String valueToken = whereClause.substring(whereClause.indexOf(operator) + operator.length()).trim();
            String whereValue = valueToken.replaceAll("^\"|\"$", "");

            DataFrame filteredDf = df.filter(whereColumn, whereValue);
            rows = new ArrayList<>();
            for (int i = 0; i < filteredDf.countRows(); i++) {
                rows.add(filteredDf.getRow(i));
            }
        } else {
            rows = new ArrayList<>();
            for (int i = 0; i < df.countRows(); i++) {
                rows.add(df.getRow(i));
            }
        }

        // Si SELECT * est demandé, on prend toutes les colonnes
        if (columnsToSelect == null) {
            columnsToSelect = new ArrayList<>(df.getColumns().keySet());
        }

        // Projection : ne garder que les colonnes demandées
        List<String> finalColumnsToSelect = columnsToSelect;
        List<Map<String, Object>> result = rows.stream().map(row -> {
            Map<String, Object> projectedRow = new LinkedHashMap<>();
            for (String col : finalColumnsToSelect) {
                projectedRow.put(col, row.getOrDefault(col, null));
            }
            return projectedRow;
        }).collect(Collectors.toList());

        return result;
    }

    // Exécution d'une requête avec agrégats
    private Map<String, Object> executeAggregateQuery(String query) {
        query = query.trim();

        int fromIndex = query.toUpperCase().indexOf(" FROM ");
        if (fromIndex == -1) {
            throw new IllegalArgumentException("La requête doit contenir FROM");
        }

        String selectPart = query.substring(7, fromIndex).trim();
        String[] selectElements = selectPart.split(",");

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

        DataFrame df = dataFrameService.getTableByName(tableName);
        if (df == null) {
            throw new IllegalArgumentException("Table " + tableName + " introuvable.");
        }

        List<Map<String, Object>> rows = new ArrayList<>();
        if (whereClause != null && !whereClause.isEmpty()) {
            String[] tokens = whereClause.split(" ");
            if (tokens.length < 3) {
                throw new IllegalArgumentException("Clause WHERE invalide.");
            }
            String whereColumn = tokens[0].trim();
            String operator = tokens[1].trim();
            String valueToken = whereClause.substring(whereClause.indexOf(operator) + operator.length()).trim();
            String whereValue = valueToken.replaceAll("^\"|\"$", "");

            DataFrame filteredDf = df.filter(whereColumn, whereValue);
            for (int i = 0; i < filteredDf.countRows(); i++) {
                rows.add(filteredDf.getRow(i));
            }
        } else {
            for (int i = 0; i < df.countRows(); i++) {
                rows.add(df.getRow(i));
            }
        }

        Map<String, Object> result = new LinkedHashMap<>();

        for (String element : selectElements) {
            element = element.trim();
            if (element.equalsIgnoreCase("COUNT(*)")) {
                result.put("COUNT", rows.size());
            } else if (element.matches("(?i)(MIN|MAX|SUM|AVG)\\(\\s*\\w+\\s*\\)")) {
                String function = element.substring(0, element.indexOf("(")).toUpperCase();
                String column = element.substring(element.indexOf("(") + 1, element.indexOf(")")).trim();
                List<Object> values = rows.stream()
                        .map(row -> row.get(column))
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList());
                switch (function) {
                    case "MIN":
                        Object min = values.stream()
                                .min((o1, o2) -> ((Comparable) o1).compareTo(o2))
                                .orElse(null);
                        result.put("MIN(" + column + ")", min);
                        break;
                    case "MAX":
                        Object max = values.stream()
                                .max((o1, o2) -> ((Comparable) o1).compareTo(o2))
                                .orElse(null);
                        result.put("MAX(" + column + ")", max);
                        break;
                    case "SUM":
                        Double sum = values.stream()
                                .mapToDouble(o -> ((Number) o).doubleValue())
                                .sum();
                        result.put("SUM(" + column + ")", sum);
                        break;
                    case "AVG":
                        Double avg = values.stream()
                                .mapToDouble(o -> ((Number) o).doubleValue())
                                .average()
                                .orElse(0);
                        result.put("AVG(" + column + ")", avg);
                        break;
                    default:
                        throw new IllegalArgumentException("Fonction inconnue: " + function);
                }
            } else {
                // Si ce n'est ni une fonction d'agrégat, on peut éventuellement renvoyer un message ou ignorer
                result.put(element, "Fonction ou colonne non supportée en agrégation");
            }
        }

        return result;
    }
}