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
     * SELECT AVG(nomColonne) FROM tableName [WHERE ...]
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

        // Détecter les agrégations SUM, COUNT et AVG
        boolean isSumQuery = false;
        String sumColumn = "";
        boolean isCountQuery = false;
        String countColumn = "";
        boolean isAvgQuery = false;
        String avgColumn = "";
        
        String selectPartUpper = selectPart.toUpperCase();
        if (selectPartUpper.startsWith("SUM(") && selectPart.endsWith(")")) {
            isSumQuery = true;
            sumColumn = selectPart.substring(4, selectPart.length() - 1).trim();
        } else if (selectPartUpper.startsWith("COUNT(") && selectPart.endsWith(")")) {
            isCountQuery = true;
            countColumn = selectPart.substring(6, selectPart.length() - 1).trim();
        } else if (selectPartUpper.startsWith("AVG(") && selectPart.endsWith(")")) {
            isAvgQuery = true;
            avgColumn = selectPart.substring(4, selectPart.length() - 1).trim();
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

        // Log more information for debugging
        System.out.println("DEBUG: Executing SQL query on table: '" + tableName + "'");
        
        // Récupérer le DataFrame via TableService
        DataFrame df = tableService.getTableByName(tableName);
        if (df == null) {
            // Better error message to debug what tables are available
            StringBuilder availableTables = new StringBuilder();
            tableService.getAllTableNames().forEach(name -> availableTables.append(name).append(", "));
            String tableList = availableTables.length() > 0 ? availableTables.substring(0, availableTables.length() - 2) : "none";
            
            throw new IllegalArgumentException("Table '" + tableName + "' not found. Available tables: " + tableList);
        }

        // Appliquer la clause WHERE (incluant eq et contains)
        List<Map<String, Object>> rows;
        if (whereClause != null && !whereClause.isEmpty()) {
            rows = applyWhereFilter(df, whereClause);
        } else {
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
        
        // Si c'est une requête AVG, calculer la moyenne et retourner le résultat
        if (isAvgQuery) {
            double sum = 0.0;
            int count = 0;
            
            for (Map<String, Object> row : rows) {
                Object value = row.get(avgColumn);
                if (value instanceof Number) {
                    sum += ((Number) value).doubleValue();
                    count++;
                } else if (value != null) {
                    try {
                        sum += Double.parseDouble(value.toString());
                        count++;
                    } catch (NumberFormatException e) {
                        // Ignorer la valeur si elle n'est pas convertible en nombre
                    }
                }
            }
            
            double avg = count > 0 ? sum / count : 0;
            Map<String, Object> avgResult = new LinkedHashMap<>();
            avgResult.put("AVG(" + avgColumn + ")", avg);
            return Collections.singletonList(avgResult);
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

        return result;
    }

    private List<Map<String, Object>> applyWhereFilter(DataFrame df, String whereClause) {
        // First check for YEAR filter 
        boolean isYearFilter = whereClause.toUpperCase().startsWith("YEAR(");
        
        // For all other cases, try to split into column, operator, and value
        String whereColumn, operator, whereValue;
        
        if (isYearFilter) {
            whereColumn = whereClause.substring(5, whereClause.indexOf(")")).trim();
            operator = whereClause.substring(whereClause.indexOf(")") + 1).trim().split(" ")[0];
            whereValue = whereClause.substring(whereClause.indexOf(operator) + operator.length()).trim();
        } else {
            // Standard WHERE format: column operator value
            String[] parts = whereClause.split("\\s+", 3);
            if (parts.length < 3) {
                throw new IllegalArgumentException("Clause WHERE invalide: " + whereClause);
            }
            
            whereColumn = parts[0].trim();
            operator = parts[1].trim().toLowerCase();
            whereValue = parts[2].trim();
            
            // Remove quotes if present
            if (whereValue.startsWith("\"") && whereValue.endsWith("\"")) {
                whereValue = whereValue.substring(1, whereValue.length() - 1);
            }
            if (whereValue.startsWith("'") && whereValue.endsWith("'")) {
                whereValue = whereValue.substring(1, whereValue.length() - 1);
            }
        }
        
        // Handle different operators
        DataFrame filteredDf = null;
        
        if (isYearFilter) {
            // Year filter case
            filteredDf = df.filterYear(whereColumn, Integer.parseInt(whereValue));
        } else if (operator.equals("eq")) {
            filteredDf = df.filter(whereColumn, whereValue);
        } else if (operator.equals("contains")) {
            filteredDf = df.filterContains(whereColumn, whereValue);
        } else if (operator.equals(">") || operator.equals("<") || 
                   operator.equals(">=") || operator.equals("<=") ||
                   operator.equals("=")) {
            // Convert value to double for numeric comparisons
            try {
                double numericValue = Double.parseDouble(whereValue);
                filteredDf = df.filterNumeric(whereColumn, operator, numericValue);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Value '" + whereValue + "' is not a valid number for comparison");
            }
        } else {
            throw new IllegalArgumentException("Opérateur WHERE invalide : " + operator);
        }
        
        // Convert the filtered DataFrame to a list of maps
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
