package com.ARYD.MemoryDB.service;

import com.ARYD.MemoryDB.entity.DataFrame;
import com.ARYD.MemoryDB.service.TableService;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
public class QueryService {

    // Supposons que TableService permet de récupérer un DataFrame par son nom
    private final TableService tableService;



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
        //     // Après FROM, on peut avoir une clause WHERE et/ou ORDER BY.
        String afterFrom = query.substring(fromIndex + 6).trim();

        // Extraire de la clause ORDER BY
        int orderByIndex = afterFrom.toUpperCase().indexOf(" ORDER BY ");
        String orderByClause = null;
        if (orderByIndex != -1) {
            orderByClause = afterFrom.substring(orderByIndex + 10).trim();
            afterFrom = afterFrom.substring(0, orderByIndex).trim();
        }

        // Extraire LIMIT
        int limitIndex = afterFrom.toUpperCase().indexOf(" LIMIT ");
        String limitClause = null;
        if (limitIndex != -1) {
            limitClause = afterFrom.substring(limitIndex + 7).trim();
            afterFrom = afterFrom.substring(0, limitIndex).trim(); // Supprime LIMIT de la requête
        }

        //la partie FROM et WHERE
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
        DataFrame df = tableService.getTableByName(tableName);
        if (df == null) {
            throw new IllegalArgumentException("Table " + tableName + " introuvable.");
        }

        // Appliquer la clause WHERE (incluant eq et contains)
        List<Map<String, Object>> rows;
        if (whereClause != null && !whereClause.isEmpty()) {
            rows = applyWhereFilter(df, whereClause);
        }else{
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


        // Appel  ORDER BY
        result = applyOrderBy(result, orderByClause);
        //appel LIMIT
        result = applyLimit(result, limitClause);

        if (query.toUpperCase().contains(" GROUP BY ")) {
            List<Map<String, Object>> groupByResults = processGroupBy(query, rows, selectPart);
            groupByResults = applyOrderBy(groupByResults, orderByClause);
            groupByResults = applyLimit(groupByResults, limitClause);
            return groupByResults;
        }

        return result;
    }
    private List<Map<String, Object>> processGroupBy(String query, List<Map<String, Object>> rows, String selectPart) {
        // Récupérer la clause GROUP BY
        int groupByIndex = query.toUpperCase().indexOf(" GROUP BY ");
        String groupByClause = query.substring(groupByIndex + " GROUP BY ".length()).trim();

        // Support pour plusieurs colonnes GROUP BY, séparées par des virgules
        List<String> groupByColumns = Arrays.stream(groupByClause.split(","))
                .map(String::trim)
                .collect(Collectors.toList());

        // Analyser les items de la clause SELECT
        List<String> selectItems = Arrays.stream(selectPart.split(","))
                .map(String::trim)
                .collect(Collectors.toList());

        // Utiliser une clé composite (List<Object>) pour grouper par plusieurs colonnes
        Map<List<Object>, List<Map<String, Object>>> groupedMap = new LinkedHashMap<>();
        for (Map<String, Object> row : rows) {
            List<Object> key = new ArrayList<>();
            for (String col : groupByColumns) {
                key.add(row.get(col));
            }
            groupedMap.computeIfAbsent(key, k -> new ArrayList<>()).add(row);
        }

        List<Map<String, Object>> aggregatedResults = new ArrayList<>();
        // Pour chaque groupe, construire une ligne résultat
        for (Map.Entry<List<Object>, List<Map<String, Object>>> entry : groupedMap.entrySet()) {
            List<Object> groupKey = entry.getKey();
            List<Map<String, Object>> groupRows = entry.getValue();
            Map<String, Object> aggregatedRow = new LinkedHashMap<>();

            // Ajoute les colonnes de group by dans le résultat
            for (int i = 0; i < groupByColumns.size(); i++) {
                aggregatedRow.put(groupByColumns.get(i), groupKey.get(i));
            }

            // Pour chaque item SELECT qui n'est pas dans le group by
            for (String item : selectItems) {
                if (groupByColumns.contains(item)) continue;
                if (isAggregationFunction(item)) {
                    String func = item.substring(0, item.indexOf("(")).trim().toUpperCase();
                    String colName = item.substring(item.indexOf("(") + 1, item.lastIndexOf(")")).trim();
                    Object aggValue = computeSingleAggregator(groupRows, func, colName);
                    aggregatedRow.put(item, aggValue);
                } else {
                    // Pour une colonne non agrégée, on prend la valeur du premier row du groupe.
                    aggregatedRow.put(item, groupRows.get(0).get(item));
                }
            }
            aggregatedResults.add(aggregatedRow);
        }
        return aggregatedResults;
    }


    private Object computeSingleAggregator(List<Map<String, Object>> rows, String func, String colName) {
        switch (func) {
            case "COUNT":
                if (colName.equals("*")) {
                    return rows.size();
                } else {
                    long count = rows.stream().filter(r -> r.get(colName) != null).count();
                    return (int) count;
                }
            case "SUM":
                double sum = 0.0;
                for (Map<String, Object> row : rows) {
                    Object value = row.get(colName);
                    sum += parseDouble(value);
                }
                return sum;
            case "AVG":
                double total = 0.0;
                int cnt = 0;
                for (Map<String, Object> row : rows) {
                    Object value = row.get(colName);
                    if (value != null) {
                        total += parseDouble(value);
                        cnt++;
                    }
                }
                return (cnt == 0) ? 0.0 : (total / cnt);
            case "MIN":
                Double minValue = null;
                for (Map<String, Object> row : rows) {
                    Object value = row.get(colName);
                    if (value != null) {
                        double d = parseDouble(value);
                        if (minValue == null || d < minValue) {
                            minValue = d;
                        }
                    }
                }
                return (minValue != null) ? minValue : 0.0;
            case "MAX":
                Double maxValue = null;
                for (Map<String, Object> row : rows) {
                    Object value = row.get(colName);
                    if (value != null) {
                        double d = parseDouble(value);
                        if (maxValue == null || d > maxValue) {
                            maxValue = d;
                        }
                    }
                }
                return (maxValue != null) ? maxValue : 0.0;
            default:
                throw new IllegalArgumentException("Hàm d'agrégation non supportée: " + func);
        }
    }

    private boolean isAggregationFunction(String item) {
        String upper = item.toUpperCase();
        return (upper.startsWith("SUM(") || upper.startsWith("COUNT(") || upper.startsWith("AVG(")
                || upper.startsWith("MIN(") || upper.startsWith("MAX(")) && upper.endsWith(")");
    }


    private double parseDouble(Object value) {
        if (value == null) return 0.0;
        if (value instanceof Number) {
            return ((Number) value).doubleValue();
        }
        try {
            return Double.parseDouble(value.toString());
        } catch (NumberFormatException e) {
            return 0.0;
        }
    }


    private List<Map<String, Object>> applyWhereFilter(DataFrame df, String whereClause) {
        boolean isYearFilter = whereClause.toUpperCase().startsWith("YEAR(");
        String whereColumn, operator, whereValue;

        if (isYearFilter) {
            whereColumn = whereClause.substring(5, whereClause.indexOf(")")).trim();
            operator = whereClause.substring(whereClause.indexOf(")") + 1).trim().split(" ")[0];
            whereValue = whereClause.substring(whereClause.indexOf(operator) + operator.length()).trim();
        } else {
            String[] tokens = whereClause.split(" ");
            if (tokens.length < 3) throw new IllegalArgumentException("Clause WHERE invalide.");
            whereColumn = tokens[0].trim();
            operator = tokens[1].trim();
            whereValue = whereClause.substring(whereClause.indexOf(operator) + operator.length()).trim();
        }

        DataFrame filteredDf;
        if (isYearFilter) {
            filteredDf = df.filterYear(whereColumn, Integer.parseInt(whereValue));
        } else if (operator.equalsIgnoreCase("eq")) {
            filteredDf = df.filter(whereColumn, whereValue);
        } else if (operator.equalsIgnoreCase("contains")) {
            filteredDf = df.filterContains(whereColumn, whereValue);
        } else {
            throw new IllegalArgumentException("Opérateur WHERE invalide : " + operator);
        }

        // return List Map<String, Object> from DataFrame
        List<Map<String, Object>> rows = new ArrayList<>();
        for (int i = 0; i < filteredDf.countRows(); i++) {
            rows.add(filteredDf.getRow(i));
        }
        return rows;
    }



    private List<Map<String, Object>> applyOrderBy(List<Map<String, Object>> result, String orderByClause) {
        // Vérifier si ORDER BY est vide ou si la liste est vide → Pas besoin de trier
        if (orderByClause == null || orderByClause.isEmpty() || result.isEmpty()) return result;

        // Séparer la clause ORDER BY en mots : ex. "prix DESC" → ["prix", "DESC"]
        String[] orderTokens = orderByClause.split(" ");
        if (orderTokens.length == 0) throw new IllegalArgumentException("La clause ORDER BY est invalide.");

        // Récupérer le nom de la colonne à trier
        String orderByColumn = orderTokens[0].trim();

        // Déterminer la direction du tri (ASC par défaut si non spécifiée)
        String direction = (orderTokens.length > 1) ? orderTokens[1].trim().toUpperCase() : "ASC";

        // Vérifier si la direction est valide (ASC ou DESC uniquement)
        if (!direction.equals("ASC") && !direction.equals("DESC"))
            throw new IllegalArgumentException("Valeur ORDER BY invalide : " + direction + ". Utiliser ASC ou DESC.");

        // Vérifier si la colonne demandée existe dans les résultats
        if (!result.get(0).containsKey(orderByColumn))
            throw new IllegalArgumentException("La colonne '" + orderByColumn + "' n'existe pas dans les résultats.");

        // Convertir direction en variable finale pour l'utiliser dans la lambda
        final String finalDirection = direction;

        // Trier la liste des résultats selon la colonne spécifiée
        result.sort(Comparator.comparing(
                map -> (Comparable) map.get(orderByColumn), // Extraire la valeur de la colonne pour chaque ligne
                (val1, val2) -> {
                    // Gérer les valeurs nulles pour éviter les erreurs de comparaison
                    if (val1 == null && val2 == null) return 0; // Deux valeurs nulles → Égalité
                    if (val1 == null) return finalDirection.equals("ASC") ? -1 : 1; // Null vient en premier ou dernier
                    if (val2 == null) return finalDirection.equals("ASC") ? 1 : -1;

                    // Comparer normalement selon la direction ASC ou DESC
                    return finalDirection.equals("ASC") ? val1.compareTo(val2) : val2.compareTo(val1);
                }
        ));

        return result; // Retourner la liste triée
    }

    private List<Map<String, Object>> applyLimit(List<Map<String, Object>> result, String limitClause) {
        if (limitClause == null || limitClause.isEmpty()) return result; // Pas de LIMIT

        try {
            int limit = Integer.parseInt(limitClause.trim());
            if (limit < 0) throw new IllegalArgumentException("LIMIT doit être un entier positif.");
            return result.subList(0, Math.min(limit, result.size())); // Retourne max 'limit' éléments
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Valeur LIMIT invalide : " + limitClause);
        }
    }


}
