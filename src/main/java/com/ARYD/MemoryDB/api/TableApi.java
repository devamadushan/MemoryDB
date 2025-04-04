package com.ARYD.MemoryDB.api;

import com.ARYD.MemoryDB.service.ParquetService;
import com.ARYD.MemoryDB.service.QueryService;
import com.ARYD.MemoryDB.storage.Table;
import com.ARYD.MemoryDB.storage.ColumnDefinition;
import com.ARYD.MemoryDB.query.QueryEngine;
import com.ARYD.MemoryDB.util.ParquetInspector;
import com.ARYD.entity.QueryResult;
import com.ARYD.entity.LoadingStatus;
import com.ARYD.entity.TableCreationRequest;
import com.ARYD.entity.DistributedLoadingRequest;
import com.ARYD.network.ClusterConfig;
import com.ARYD.network.ClusterManager;
import com.ARYD.network.ServerNode;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

import java.net.http.HttpResponse;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.io.StringWriter;
import java.io.PrintWriter;
import java.util.Objects;

/**
 * API REST pour les opérations sur les tables
 */
@RestController
@RequestMapping("/api/tables")
@RequiredArgsConstructor
@Slf4j
public class TableApi {
    private static final Map<String, LoadingStatus> loadingStatuses = new ConcurrentHashMap<>();
    
    private final ParquetService parquetService;
    private final QueryEngine queryEngine;
    private final QueryService queryService;
    private final ParquetInspector parquetInspector;
    private final ClusterManager clusterManager;
    private final Map<String, Table> tables = new HashMap<>();

    /**
     * Liste toutes les tables disponibles
     */
    @GetMapping
    public ResponseEntity<Map<String, Object>> getAllTables() {
        try {
            // Récupérer les tables locales
            List<Map<String, Object>> localTables = tables.values().stream()
                    .map(table -> {
                        Map<String, Object> tableInfo = new HashMap<>();
                        tableInfo.put("name", table.getName());
                        tableInfo.put("columns", table.getColumnNames());
                        tableInfo.put("rowCount", table.getRowCount());
                        tableInfo.put("isDistributed", true);
                        tableInfo.put("nodeLocation", ClusterConfig.getInstance().getCurrentNodeName());
                        return tableInfo;
                    })
                    .collect(Collectors.toList());
            
            // Récupérer les tables des autres nœuds
            List<Map<String, Object>> allTables = new ArrayList<>(localTables);
            
            if (ClusterConfig.getInstance().getAllNodes().size() > 1) {
                try {
                    // Appel asynchrone aux autres nœuds pour récupérer leurs tables
                    CompletableFuture<List<HttpResponse<String>>> futureResponses = clusterManager.broadcast("/api/tables/local", "GET", null);
                    
                    // Attendre et agréger les résultats
                    List<HttpResponse<String>> responses = futureResponses.join();
                    for (HttpResponse<String> response : responses) {
                        if (response.statusCode() == 200) {
                            // Convertir la réponse en Map
                            String responseBody = response.body();
                            try {
                                // Note: Dans une implémentation réelle, utilisez Jackson ou Gson
                                Map<String, Object> remoteResponse = parseJson(responseBody);
                                List<Map<String, Object>> remoteTables = (List<Map<String, Object>>) remoteResponse.get("tables");
                                if (remoteTables != null) {
                                    allTables.addAll(remoteTables);
                                }
                            } catch (Exception e) {
                                log.warn("Erreur lors du parsing de la réponse JSON: {}", e.getMessage());
                            }
                        }
                    }
                } catch (Exception e) {
                    log.warn("Erreur lors de la récupération des tables distantes: {}", e.getMessage());
                    // Continuer avec uniquement les tables locales en cas d'erreur
                }
            }
            
            Map<String, Object> response = new HashMap<>();
            response.put("tables", allTables);
            response.put("count", allTables.size());
            
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            log.error("Erreur lors de la récupération des tables", e);
            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("error", "Erreur lors de la récupération des tables: " + e.getMessage());
            return ResponseEntity.internalServerError().body(errorResponse);
        }
    }
    
    /**
     * Endpoint pour récupérer uniquement les tables locales (utilisé pour la communication interne)
     */
    @GetMapping("/local")
    public ResponseEntity<Map<String, Object>> getLocalTables(@RequestHeader("InternalToken") String token) {
        // Vérifier si c'est un appel interne
        if (token == null || !ClusterConfig.getInstance().isValidToken(token)) {
            log.warn("Tentative non autorisée d'accès à l'API interne");
            return ResponseEntity.status(401).build();
        }
        
        try {
            List<Map<String, Object>> localTables = tables.values().stream()
                    .map(table -> {
                        Map<String, Object> tableInfo = new HashMap<>();
                        tableInfo.put("name", table.getName());
                        tableInfo.put("columns", table.getColumnNames());
                        tableInfo.put("rowCount", table.getRowCount());
                        tableInfo.put("isDistributed", true);
                        tableInfo.put("nodeLocation", ClusterConfig.getInstance().getCurrentNodeName());
                        return tableInfo;
                    })
                    .collect(Collectors.toList());
            
            Map<String, Object> response = new HashMap<>();
            response.put("tables", localTables);
            response.put("count", localTables.size());
            
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            log.error("Erreur lors de la récupération des tables locales", e);
            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("error", "Erreur lors de la récupération des tables locales: " + e.getMessage());
            return ResponseEntity.internalServerError().body(errorResponse);
        }
    }
    
    /**
     * Obtient des informations sur une table spécifique
     */
    @GetMapping("/{tableName}")
    public ResponseEntity<Map<String, Object>> getTableInfo(@PathVariable String tableName) {
        Table table = tables.get(tableName);
        if (table == null) {
            return ResponseEntity.notFound().build();
        }

        Map<String, Object> response = new HashMap<>();
        response.put("tableName", table.getName());
        response.put("rowCount", table.getRowCount());
        response.put("columnCount", table.getColumnCount());
        response.put("columnNames", table.getColumnNames());
        response.put("memoryUsage", table.estimateMemoryUsage());

        return ResponseEntity.ok(response);
    }

    /**
     * Inspection d'un fichier Parquet
     */
    @GetMapping("/inspect-parquet")
    public ResponseEntity<Map<String, Object>> inspectParquetFile(@RequestParam String filePath) {
        try {
            log.info("Inspection du fichier Parquet: {}", filePath);
            Map<String, Object> inspectionResult = parquetInspector.inspectParquetFile(filePath);
            
            if (inspectionResult.containsKey("error")) {
                return ResponseEntity.badRequest().body(inspectionResult);
            }
            
            return ResponseEntity.ok(inspectionResult);
        } catch (Exception e) {
            log.error("Erreur lors de l'inspection du fichier Parquet", e);
            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("error", "Erreur lors de l'inspection du fichier Parquet");
            errorResponse.put("message", e.getMessage() != null ? e.getMessage() : "Erreur inconnue");
            errorResponse.put("filePath", filePath);
            return ResponseEntity.internalServerError().body(errorResponse);
        }
    }

    /**
     * Charge un fichier Parquet dans une table
     */
    @PostMapping("/load-parquet")
    public ResponseEntity<Map<String, Object>> loadParquetFile(
            @RequestParam String filePath,
            @RequestParam(required = false) String tableName,
            @RequestParam(required = false, defaultValue = "-1") int maxRows,
            @RequestParam(required = false, defaultValue = "false") boolean async) {
        try {
            log.info("Chargement du fichier Parquet: {}, tableName: {}, maxRows: {}, async: {}", 
                     filePath, tableName != null ? tableName : "auto", maxRows == -1 ? "illimité" : maxRows, async);
            
            // Valider que le fichier est un fichier Parquet valide
            if (!parquetInspector.isValidParquetFile(filePath)) {
                Map<String, Object> errorResponse = new HashMap<>();
                errorResponse.put("error", "Le fichier n'est pas un fichier Parquet valide");
                errorResponse.put("filePath", filePath);
                return ResponseEntity.badRequest().body(errorResponse);
            }
            
            if (async) {
                // Lancer le chargement en mode asynchrone
                String loadingId = java.util.UUID.randomUUID().toString();
                LoadingStatus status = new LoadingStatus(loadingId, filePath, "STARTED", 0, 0);
                loadingStatuses.put(loadingId, status);
                
                CompletableFuture.runAsync(() -> {
                    try {
                        // Mettre à jour le statut
                        status.setStatus("LOADING");
                        loadingStatuses.put(loadingId, status);
                        
                        // Charger la table
                        Table table = parquetService.readParquetFile(filePath, maxRows, tableName);
                        
                        if (table != null) {
                            // Enregistrer la table dans le contrôleur et le moteur de requêtes
                            tables.put(table.getName(), table);
                            queryEngine.registerTable(table);
                            
                            status.setStatus("COMPLETED");
                            status.setRowCount((int)table.getRowCount());
                        } else {
                            status.setStatus("FAILED");
                            status.setErrorMessage("Impossible de charger le fichier Parquet");
                        }
                    } catch (Exception e) {
                        status.setStatus("FAILED");
                        status.setErrorMessage(e.getMessage());
                        log.error("Erreur lors du chargement asynchrone", e);
                    }
                });
                
                // Retourner immédiatement avec l'ID de chargement
                Map<String, Object> response = new HashMap<>();
                response.put("loadingId", loadingId);
                response.put("status", "STARTED");
                response.put("filePath", filePath);
                return ResponseEntity.accepted().body(response);
            } else {
                // Chargement synchrone
                Table table = parquetService.readParquetFile(filePath, maxRows, tableName);
                if (table == null) {
                    Map<String, Object> errorResponse = new HashMap<>();
                    errorResponse.put("error", "Impossible de charger le fichier Parquet");
                    errorResponse.put("filePath", filePath);
                    return ResponseEntity.badRequest().body(errorResponse);
                }

                // Enregistrer la table dans le contrôleur et le moteur de requêtes
                tables.put(table.getName(), table);
                queryEngine.registerTable(table);

                // Retourner les informations sur la table
                Map<String, Object> response = new HashMap<>();
                response.put("tableName", table.getName());
                response.put("rowCount", table.getRowCount());
                response.put("columnCount", table.getColumnCount());
                response.put("columnNames", table.getColumnNames());
                response.put("memoryUsage", table.estimateMemoryUsage());
                
                // Si on a utilisé une limite, indiquer que le chargement pourrait être partiel
                if (maxRows > 0 && table.getRowCount() >= maxRows) {
                    response.put("notice", "Le chargement a été limité à " + maxRows + " lignes. Le fichier pourrait contenir plus de données.");
                }

                return ResponseEntity.ok(response);
            }
        } catch (Exception e) {
            log.error("Erreur lors du chargement du fichier Parquet", e);
            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("error", "Erreur lors du chargement du fichier Parquet");
            errorResponse.put("message", e.getMessage() != null ? e.getMessage() : "Erreur inconnue");
            errorResponse.put("filePath", filePath);
            return ResponseEntity.internalServerError().body(errorResponse);
        }
    }

    /**
     * Vérifie le statut d'un chargement asynchrone
     */
    @GetMapping("/loading-status/{loadingId}")
    public ResponseEntity<LoadingStatus> getLoadingStatus(@PathVariable String loadingId) {
        LoadingStatus status = loadingStatuses.get(loadingId);
        if (status == null) {
            return ResponseEntity.notFound().build();
        }
        
        return ResponseEntity.ok(status);
    }

    /**
     * Obtient un échantillon de données d'une table
     */
    @GetMapping("/{tableName}/sample")
    public ResponseEntity<Map<String, Object>> getTableSample(
            @PathVariable String tableName,
            @RequestParam(defaultValue = "10") int limit) {
        
        Table table = tables.get(tableName);
        if (table == null) {
            return ResponseEntity.notFound().build();
        }

        // Utiliser le moteur de requêtes pour obtenir un échantillon
        Table sample = queryEngine.limit(tableName, limit, 0);

        Map<String, Object> response = new HashMap<>();
        response.put("tableName", tableName);
        response.put("sampleSize", sample.getRowCount());
        response.put("columns", sample.getColumnNames());
        
        // Récupérer les données échantillonnées
        List<Map<String, Object>> rows = new ArrayList<>();
        for (int i = 0; i < sample.getRowCount(); i++) {
            rows.add(sample.getRow(i));
        }
        response.put("rows", rows);

        return ResponseEntity.ok(response);
    }

    /**
     * Récupère des données d'une table avec pagination
     */
    @GetMapping("/{tableName}/data")
    public ResponseEntity<Map<String, Object>> getTableData(
            @PathVariable String tableName,
            @RequestParam(defaultValue = "100") int limit,
            @RequestParam(defaultValue = "0") int offset) {
        
        Table table = tables.get(tableName);
        if (table == null) {
            return ResponseEntity.notFound().build();
        }

        // Utiliser le moteur de requêtes pour obtenir les données avec pagination
        Table result = queryEngine.limit(tableName, limit, offset);

        Map<String, Object> response = new HashMap<>();
        response.put("tableName", tableName);
        response.put("totalRows", table.getRowCount());
        response.put("returnedRows", result.getRowCount());
        response.put("offset", offset);
        response.put("columns", result.getColumnNames());
        
        // Récupérer les données
        List<Map<String, Object>> rows = new ArrayList<>();
        for (int i = 0; i < result.getRowCount(); i++) {
            rows.add(result.getRow(i));
        }
        response.put("rows", rows);

        return ResponseEntity.ok(response);
    }

    /**
     * Exporte les données d'une table au format CSV
     */
    @GetMapping("/{tableName}/export-csv")
    public ResponseEntity<String> exportToCsv(
            @PathVariable String tableName,
            @RequestParam(defaultValue = "-1") int limit) {
        
        log.info("Demande d'export CSV pour la table: {}", tableName);
        Table table = tables.get(tableName);
        if (table == null) {
            log.warn("Table non trouvée: {}", tableName);
            return ResponseEntity.notFound().build();
        }

        // Si limit est -1, exporter toute la table
        int rowsToExport = (limit == -1) ? table.getRowCount() : Math.min(limit, table.getRowCount());
        log.info("Export de {} lignes pour la table {}", rowsToExport, tableName);
        
        StringWriter stringWriter = new StringWriter();
        PrintWriter writer = new PrintWriter(stringWriter);
        
        // Écrire l'en-tête CSV avec les noms des colonnes
        List<String> columnNames = table.getColumnNames();
        for (int i = 0; i < columnNames.size(); i++) {
            writer.print(escapeSpecialCsvChars(columnNames.get(i)));
            if (i < columnNames.size() - 1) {
                writer.print(",");
            }
        }
        writer.println();
        
        // Écrire les données
        for (int i = 0; i < rowsToExport; i++) {
            Map<String, Object> row = table.getRow(i);
            for (int j = 0; j < columnNames.size(); j++) {
                String columnName = columnNames.get(j);
                Object value = row.get(columnName);
                writer.print(escapeSpecialCsvChars(value != null ? value.toString() : ""));
                if (j < columnNames.size() - 1) {
                    writer.print(",");
                }
            }
            writer.println();
        }
        
        // Configurer les en-têtes de la réponse pour le téléchargement
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.TEXT_PLAIN);
        headers.set(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=" + tableName + ".csv");
        
        log.info("Export CSV terminé avec succès pour la table: {}", tableName);
        return ResponseEntity.ok()
                .headers(headers)
                .body(stringWriter.toString());
    }

    /**
     * Exécute une requête SQL sur une table
     */
    @PostMapping("/{tableName}/query")
    public ResponseEntity<Map<String, Object>> executeTableQuery(
            @PathVariable String tableName,
            @RequestBody Map<String, Object> queryParams) {
        
        long startTime = System.currentTimeMillis();
        Table table = tables.get(tableName);
        if (table == null) {
            return ResponseEntity.notFound().build();
        }

        try {
            // Extraire les paramètres de la requête
            List<String> selectColumns = (List<String>) queryParams.getOrDefault("select", table.getColumnNames());
            Map<String, Object> filters = (Map<String, Object>) queryParams.getOrDefault("where", Map.of());
            String groupBy = (String) queryParams.getOrDefault("groupBy", null);
            List<String> orderBy = (List<String>) queryParams.getOrDefault("orderBy", List.of());
            boolean descending = (boolean) queryParams.getOrDefault("desc", false);
            int limit = (int) queryParams.getOrDefault("limit", 100);
            int offset = (int) queryParams.getOrDefault("offset", 0);
            
            // Exécuter la requête via le moteur de requêtes
            // Utiliser une combinaison de méthodes existantes au lieu de executeQuery
            // On commence par obtenir un résultat filtré/groupé avec la requête SQL
            String sql = buildSqlQuery(tableName, selectColumns, filters, groupBy, orderBy, descending);
            // Utiliser le queryService au lieu de queryEngine pour exécuter du SQL
            List<Map<String, Object>> queryResults = queryService.executeQuery(sql);
            
            // Convertir les résultats en un objet Table ou utiliser directement limit
            // Pour simplifier, on peut utiliser directement les résultats
            Table result;
            if (offset > 0 || queryResults.size() > limit) {
                // Si besoin de pagination, utiliser limit
                int endIndex = Math.min(offset + limit, queryResults.size());
                if (offset < queryResults.size()) {
                    // Créer une table temporaire ou utiliser les résultats directement
                    result = queryEngine.limit(tableName, limit, offset);
                } else {
                    // Si offset dépasse la taille des résultats, créer une table vide
                    // Nous n'avons pas de méthode createEmptyTable(), donc utiliser une approche alternative
                    result = queryEngine.limit(tableName, 0, 0); // Demander 0 lignes pour avoir une table vide
                }
            } else {
                // Sinon, utiliser tous les résultats
                result = queryEngine.limit(tableName, queryResults.size(), 0);
            }
            
            // Construire la réponse
            Map<String, Object> response = new HashMap<>();
            response.put("tableName", tableName);
            response.put("totalRows", table.getRowCount());
            response.put("returnedRows", result.getRowCount());
            response.put("columns", result.getColumnNames());
            response.put("executionTimeMs", System.currentTimeMillis() - startTime);
            
            // Récupérer les données
            List<Map<String, Object>> rows = new ArrayList<>();
            for (int i = 0; i < result.getRowCount(); i++) {
                rows.add(result.getRow(i));
            }
            response.put("rows", rows);
            
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            log.error("Erreur lors de l'exécution de la requête", e);
            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("error", "Erreur lors de l'exécution de la requête");
            errorResponse.put("message", e.getMessage());
            errorResponse.put("executionTimeMs", System.currentTimeMillis() - startTime);
            return ResponseEntity.badRequest().body(errorResponse);
        }
    }

    /**
     * Supprime une table
     */
    @DeleteMapping("/{tableName}")
    public ResponseEntity<Void> deleteTable(@PathVariable String tableName) {
        Table table = tables.remove(tableName);
        if (table == null) {
            return ResponseEntity.notFound().build();
        }

        queryEngine.unregisterTable(tableName);
        return ResponseEntity.ok().build();
    }

    /**
     * Liste toutes les tables disponibles (endpoint alternatif)
     */
    @GetMapping("/list")
    public ResponseEntity<Map<String, Object>> listTables() {
        Map<String, Object> response = new HashMap<>();
        response.put("tables", tables.keySet());
        return ResponseEntity.ok(response);
    }

    /**
     * Compte le nombre de lignes dans un fichier Parquet
     */
    @GetMapping("/count-rows")
    public ResponseEntity<Map<String, Object>> countParquetFileRows(
            @RequestParam String filePath,
            @RequestParam(required = false, defaultValue = "true") boolean useFastCount) {
        try {
            log.info("Comptage des lignes dans le fichier Parquet: {}, mode rapide: {}", filePath, useFastCount);
            
            // Valider que le fichier est un fichier Parquet valide
            if (!parquetInspector.isValidParquetFile(filePath)) {
                Map<String, Object> errorResponse = new HashMap<>();
                errorResponse.put("error", "Le fichier n'est pas un fichier Parquet valide");
                errorResponse.put("filePath", filePath);
                return ResponseEntity.badRequest().body(errorResponse);
            }
            
            // Compter les lignes
            long rowCount = parquetInspector.countRowsInParquetFile(filePath, useFastCount);
            
            if (rowCount < 0) {
                Map<String, Object> errorResponse = new HashMap<>();
                errorResponse.put("error", "Impossible de compter les lignes dans le fichier Parquet");
                errorResponse.put("filePath", filePath);
                return ResponseEntity.badRequest().body(errorResponse);
            }
            
            // Retourner le résultat
            Map<String, Object> response = new HashMap<>();
            response.put("filePath", filePath);
            response.put("rowCount", rowCount);
            response.put("countMethod", useFastCount ? "estimation (rapide)" : "comptage précis (lent)");
            
            if (useFastCount) {
                response.put("note", "Ce nombre est une estimation basée sur les métadonnées du fichier. Pour un comptage précis, utilisez useFastCount=false.");
            }
            
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            log.error("Erreur lors du comptage des lignes dans le fichier Parquet", e);
            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("error", "Erreur lors du comptage des lignes");
            errorResponse.put("message", e.getMessage() != null ? e.getMessage() : "Erreur inconnue");
            errorResponse.put("filePath", filePath);
            return ResponseEntity.internalServerError().body(errorResponse);
        }
    }
    
    /**
     * Create a table structure manually with column definitions
     */
    @PostMapping("/create")
    public ResponseEntity<Map<String, Object>> createTable(@RequestBody TableCreationRequest request) {
        try {
            log.info("Creating table structure: {}, distributed: {}", 
                    request.getTableName(), request.isDistributed());
            
            if (tables.containsKey(request.getTableName())) {
                Map<String, Object> errorResponse = new HashMap<>();
                errorResponse.put("error", "Table already exists");
                errorResponse.put("tableName", request.getTableName());
                return ResponseEntity.badRequest().body(errorResponse);
            }
            
            // Create table locally
            Table table = createLocalTable(request.getTableName(), request.getColumns());
            
            // If this is a distributed table, propagate to other nodes
            if (request.isDistributed()) {
                // Set special header to avoid infinite loops when propagating
                HttpHeaders headers = new HttpHeaders();
                headers.set("X-Internal-Request", "true");
                headers.set("X-Source-Node", ClusterConfig.getInstance().getCurrentNodeName());
                
                // Propagate to other nodes
                clusterManager.propagateTableCreation(request, headers);
                log.info("Table creation propagated to other nodes in the cluster");
            }
            
            Map<String, Object> response = new HashMap<>();
            response.put("tableName", table.getName());
            response.put("distributed", request.isDistributed());
            response.put("columns", table.getColumnNames());
            response.put("status", "created");
            
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            log.error("Error creating table", e);
            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("error", "Error creating table: " + e.getMessage());
            return ResponseEntity.internalServerError().body(errorResponse);
        }
    }
    
    /**
     * Create a table structure from a Parquet file schema without loading data
     */
    @PostMapping("/create-table-schema")
    public ResponseEntity<Map<String, Object>> createTableFromSchema(@RequestBody Map<String, Object> request) {
        try {
            String tableName = (String) request.get("tableName");
            String sourceFilePath = (String) request.get("sourceFilePath");
            boolean createOnly = (boolean) request.getOrDefault("createOnly", true);
            
            log.info("Creating table from Parquet schema: {}, source: {}", tableName, sourceFilePath);
            
            if (tables.containsKey(tableName)) {
                Map<String, Object> errorResponse = new HashMap<>();
                errorResponse.put("error", "Table already exists");
                errorResponse.put("tableName", tableName);
                return ResponseEntity.badRequest().body(errorResponse);
            }
            
            // Inspect Parquet file to get schema
            Map<String, Object> schema = parquetInspector.inspectParquetFile(sourceFilePath);
            if (schema.containsKey("error")) {
                return ResponseEntity.badRequest().body(schema);
            }
            
            // Convert schema to column definitions
            List<Map<String, Object>> schemaFields = (List<Map<String, Object>>) schema.get("schema");
            List<ColumnDefinition> columns = new ArrayList<>();
            
            for (Map<String, Object> field : schemaFields) {
                String name = (String) field.get("name");
                String type = (String) field.get("type");
                boolean nullable = (boolean) field.getOrDefault("nullable", true);
                
                // Convert to our internal type system if needed
                String internalType = convertToInternalType(type);
                
                columns.add(ColumnDefinition.of(name, internalType, nullable, false));
            }
            
            // Create table with the schema
            Table table = createLocalTable(tableName, columns);
            
            // Register the table
            tables.put(tableName, table);
            queryEngine.registerTable(table);
            
            // If this is a distributed table, propagate to other nodes
            boolean isDistributed = (boolean) request.getOrDefault("distributed", true);
            if (isDistributed) {
                // Create a TableCreationRequest to propagate
                TableCreationRequest creationRequest = new TableCreationRequest();
                creationRequest.setTableName(tableName);
                creationRequest.setColumns(columns);
                creationRequest.setDistributed(true);
                
                // Set special header to avoid infinite loops when propagating
                HttpHeaders headers = new HttpHeaders();
                headers.set("X-Internal-Request", "true");
                headers.set("X-Source-Node", ClusterConfig.getInstance().getCurrentNodeName());
                
                // Propagate to other nodes
                clusterManager.propagateTableCreation(creationRequest, headers);
                log.info("Table creation propagated to other nodes in the cluster");
            }
            
            Map<String, Object> response = new HashMap<>();
            response.put("tableName", tableName);
            response.put("distributed", isDistributed);
            response.put("columns", table.getColumnNames());
            response.put("columnCount", table.getColumnCount());
            response.put("status", "created");
            
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            log.error("Error creating table from schema", e);
            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("error", "Error creating table from schema: " + e.getMessage());
            return ResponseEntity.internalServerError().body(errorResponse);
        }
    }
    
    /**
     * Load data into a distributed table with distribution strategy
     */
    @PostMapping("/load-parquet-distributed")
    public ResponseEntity<Map<String, Object>> loadParquetDistributed(@RequestBody DistributedLoadingRequest request) {
        try {
            String tableName = request.getTableName();
            String filePath = request.getFilePath();
            int maxRows = request.getMaxRows();
            String distributionStrategy = request.getDistributionStrategy();
            
            log.info("Loading data with distribution strategy: {}, table: {}, file: {}", 
                    distributionStrategy, tableName, filePath);
            
            Table table = tables.get(tableName);
            if (table == null) {
                Map<String, Object> errorResponse = new HashMap<>();
                errorResponse.put("error", "Table not found");
                errorResponse.put("tableName", tableName);
                return ResponseEntity.badRequest().body(errorResponse);
            }
            
            // Validate Parquet file
            if (!parquetInspector.isValidParquetFile(filePath)) {
                Map<String, Object> errorResponse = new HashMap<>();
                errorResponse.put("error", "Invalid Parquet file");
                errorResponse.put("filePath", filePath);
                return ResponseEntity.badRequest().body(errorResponse);
            }
            
            // Distributed loading - depends on strategy
            String loadingId = UUID.randomUUID().toString();
            LoadingStatus status = new LoadingStatus(loadingId, filePath, "STARTED", 0, 0);
            loadingStatuses.put(loadingId, status);
            
            // Start loading asynchronously
            CompletableFuture.runAsync(() -> {
                try {
                    status.setStatus("LOADING");
                    loadingStatuses.put(loadingId, status);
                    
                    // Use appropriate distribution strategy
                    if ("ROUND_ROBIN".equals(distributionStrategy)) {
                        parquetService.loadParquetWithRoundRobin(filePath, tableName, maxRows, loadingId);
                    } else if ("HASH".equals(distributionStrategy) && request.getDistributionColumn() != null) {
                        parquetService.loadParquetWithHashDistribution(
                                filePath, tableName, maxRows, request.getDistributionColumn(), loadingId);
                    } else {
                        // Default to round-robin if strategy not recognized
                        parquetService.loadParquetWithRoundRobin(filePath, tableName, maxRows, loadingId);
                    }
                    
                    // Update loading status
                    status.setStatus("COMPLETED");
                    int totalRows = parquetService.getTotalRowsLoaded(loadingId);
                    status.setRowCount(totalRows);
                    loadingStatuses.put(loadingId, status);
                } catch (Exception e) {
                    log.error("Error in distributed loading", e);
                    status.setStatus("FAILED");
                    status.setErrorMessage(e.getMessage());
                    loadingStatuses.put(loadingId, status);
                }
            });
            
            // Return response with loading ID
            Map<String, Object> response = new HashMap<>();
            response.put("loadingId", loadingId);
            response.put("status", "STARTED");
            response.put("distributionStrategy", distributionStrategy);
            response.put("tableName", tableName);
            response.put("filePath", filePath);
            
            return ResponseEntity.accepted().body(response);
        } catch (Exception e) {
            log.error("Error in distributed loading request", e);
            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("error", "Error in distributed loading: " + e.getMessage());
            return ResponseEntity.internalServerError().body(errorResponse);
        }
    }
    
    /**
     * Get table statistics across the cluster
     */
    @GetMapping("/{tableName}/stats")
    public ResponseEntity<Map<String, Object>> getTableStats(@PathVariable String tableName) {
        try {
            log.info("Getting statistics for table: {}", tableName);
            
            Table table = tables.get(tableName);
            if (table == null) {
                return ResponseEntity.notFound().build();
            }
            
            // Get local statistics
            Map<String, Object> localStats = new HashMap<>();
            localStats.put("nodeName", ClusterConfig.getInstance().getCurrentNodeName());
            localStats.put("rowCount", table.getRowCount());
            localStats.put("memoryUsage", table.estimateMemoryUsage());
            
            // Get statistics from other nodes
            List<Map<String, Object>> clusterStats = new ArrayList<>();
            clusterStats.add(localStats);
            
            try {
                CompletableFuture<List<HttpResponse<String>>> futureStats = 
                        clusterManager.broadcast("/api/tables/" + tableName + "/local-stats", "GET", null);
                List<HttpResponse<String>> responses = futureStats.join();
                
                for (HttpResponse<String> response : responses) {
                    if (response.statusCode() == 200) {
                        // Parse the response and add to cluster stats
                        Map<String, Object> remoteStats = parseJson(response.body());
                        if (remoteStats != null) {
                            clusterStats.add(remoteStats);
                        }
                    }
                }
            } catch (Exception e) {
                log.warn("Error getting remote statistics: {}", e.getMessage());
                // Continue with local stats only
            }
            
            // Calculate total statistics
            long totalRows = 0;
            long totalMemory = 0;
            
            for (Map<String, Object> nodeStats : clusterStats) {
                totalRows += ((Number) nodeStats.get("rowCount")).longValue();
                totalMemory += ((Number) nodeStats.get("memoryUsage")).longValue();
            }
            
            // Build response
            Map<String, Object> response = new HashMap<>();
            response.put("tableName", tableName);
            response.put("totalRowCount", totalRows);
            response.put("totalMemoryUsage", totalMemory);
            response.put("nodeCount", clusterStats.size());
            response.put("nodesStats", clusterStats);
            
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            log.error("Error getting table statistics", e);
            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("error", "Error getting table statistics: " + e.getMessage());
            return ResponseEntity.internalServerError().body(errorResponse);
        }
    }
    
    /**
     * Get local statistics for a table (used for cluster communication)
     */
    @GetMapping("/{tableName}/local-stats")
    public ResponseEntity<Map<String, Object>> getLocalTableStats(
            @PathVariable String tableName,
            @RequestHeader("X-Internal-Request") String internalRequest) {
        
        // Validate this is an internal request
        if (!"true".equals(internalRequest)) {
            return ResponseEntity.status(403).build();
        }
        
        Table table = tables.get(tableName);
        if (table == null) {
            return ResponseEntity.notFound().build();
        }
        
        Map<String, Object> stats = new HashMap<>();
        stats.put("nodeName", ClusterConfig.getInstance().getCurrentNodeName());
        stats.put("rowCount", table.getRowCount());
        stats.put("memoryUsage", table.estimateMemoryUsage());
        
        return ResponseEntity.ok(stats);
    }
    
    /**
     * Helper method to create a local table
     */
    private Table createLocalTable(String tableName, List<ColumnDefinition> columns) {
        // Create table with the schema
        Table table = new Table(tableName, columns);
        
        // Register the table
        tables.put(tableName, table);
        queryEngine.registerTable(table);
        
        return table;
    }
    
    /**
     * Helper method to convert external type names to internal types
     */
    private String convertToInternalType(String externalType) {
        // This should match the conversion in ParquetService.convertAvroTypeToDataType
        switch (externalType.toUpperCase()) {
            case "STRING":
                return "STRING";
            case "INT":
            case "INTEGER":
                return "INT";
            case "LONG":
                return "LONG";
            case "FLOAT":
                return "FLOAT";
            case "DOUBLE":
                return "DOUBLE";
            case "BOOLEAN":
                return "BOOLEAN";
            case "BINARY":
                return "BINARY";
            case "DATE":
                return "DATE";
            case "TIMESTAMP":
            case "TIMESTAMP_MILLIS":
                return "TIMESTAMP";
            default:
                log.warn("Unknown type: {}. Using STRING as default.", externalType);
                return "STRING";
        }
    }
    
    /**
     * Utility to parse JSON string into Map
     */
    private Map<String, Object> parseJson(String json) {
        // In a real implementation, use Jackson or Gson
        // This is a placeholder for the example
        try {
            // Use a proper JSON library in the actual implementation
        return new HashMap<>();
        } catch (Exception e) {
            log.error("Error parsing JSON", e);
            return null;
        }
    }
    
    /**
     * Échappe les caractères spéciaux dans les valeurs CSV
     */
    private String escapeSpecialCsvChars(String value) {
        if (value == null) {
            return "";
        }
        
        // Si la valeur contient des virgules, des guillemets ou des sauts de ligne, l'entourer de guillemets
        if (value.contains(",") || value.contains("\"") || value.contains("\n") || value.contains("\r")) {
            // Remplacer les guillemets par des doubles guillemets
            value = value.replace("\"", "\"\"");
            // Entourer de guillemets
            return "\"" + value + "\"";
        }
        
        return value;
    }

    /**
     * Construit une requête SQL à partir des paramètres de la requête
     */
    private String buildSqlQuery(String tableName, List<String> selectColumns, Map<String, Object> filters, String groupBy, List<String> orderBy, boolean descending) {
        StringBuilder sql = new StringBuilder("SELECT ");
        sql.append(String.join(", ", selectColumns));
        sql.append(" FROM ").append(tableName);
        
        if (!filters.isEmpty()) {
            sql.append(" WHERE ");
            sql.append(buildWhereClause(filters));
        }
        
        if (groupBy != null) {
            sql.append(" GROUP BY ").append(groupBy);
        }
        
        if (!orderBy.isEmpty()) {
            sql.append(" ORDER BY ");
            sql.append(String.join(", ", orderBy));
            if (descending) {
                sql.append(" DESC");
            }
        }
        
        return sql.toString();
    }

    /**
     * Construit une clause WHERE à partir des filtres
     */
    private String buildWhereClause(Map<String, Object> filters) {
        StringBuilder whereClause = new StringBuilder();
        for (Map.Entry<String, Object> entry : filters.entrySet()) {
            String columnName = entry.getKey();
            Object value = entry.getValue();
            if (value instanceof String) {
                whereClause.append(columnName).append(" = '").append(value).append("'");
            } else if (value instanceof Number) {
                whereClause.append(columnName).append(" = ").append(value);
            } else if (value instanceof Boolean) {
                whereClause.append(columnName).append(" = ").append((Boolean) value ? 1 : 0);
            } else {
                throw new IllegalArgumentException("Type de filtre non supporté: " + value.getClass());
            }
            whereClause.append(" AND ");
        }
        whereClause.setLength(whereClause.length() - 5); // Supprimer le dernier " AND "
        return whereClause.toString();
    }

    private boolean getBooleanValue(Object value) {
        if (value instanceof Boolean) {
            return (Boolean) value;
        } else if (value instanceof Number) {
            return ((Number) value).intValue() != 0;
        } else if (value instanceof String) {
            String str = ((String) value).toLowerCase();
            return str.equals("true") || str.equals("1") || str.equals("yes");
        }
        return false;
    }

    /**
     * Receive a record from another node in the cluster for distributed loading
     */
    @PostMapping("/record")
    public ResponseEntity<Map<String, Object>> receiveRecord(
            @RequestBody com.ARYD.entity.Record record,
            @RequestHeader(value = "X-Internal-Request", required = false) String internalRequest,
            @RequestHeader(value = "X-Source-Node", required = false) String sourceNode) {
        
        try {
            // Validate that this is an internal request
            if (!"true".equals(internalRequest)) {
                Map<String, Object> errorResponse = new HashMap<>();
                errorResponse.put("error", "Unauthorized access to internal API");
                return ResponseEntity.status(403).body(errorResponse);
            }
            
            log.info("Received record for table {} from node {}", record.getTableName(), sourceNode);
            
            // Get the table
            Table table = tables.get(record.getTableName());
            if (table == null) {
                Map<String, Object> errorResponse = new HashMap<>();
                errorResponse.put("error", "Table not found: " + record.getTableName());
                return ResponseEntity.badRequest().body(errorResponse);
            }
            
            // Add the data to the table
            Map<String, Object> data = record.getData();
            for (Map.Entry<String, Object> entry : data.entrySet()) {
                table.addValue(entry.getKey(), entry.getValue());
            }
            
            // Return success response
            Map<String, Object> response = new HashMap<>();
            response.put("status", "success");
            response.put("tableName", record.getTableName());
            response.put("loadingId", record.getLoadingId());
            
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            log.error("Error processing received record", e);
            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("error", "Error processing record: " + e.getMessage());
            return ResponseEntity.internalServerError().body(errorResponse);
        }
    }
} 