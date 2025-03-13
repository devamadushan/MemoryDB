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

        // Récupérer le DataFrame via TableService (à adapter selon votre implémentation)
        DataFrame df = tableService.getTableByName(tableName);
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
            DataFrame filteredDf = df.filter(whereColumn, whereValue);
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