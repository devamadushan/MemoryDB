package com.ARYD.MemoryDB.api;

import com.ARYD.MemoryDB.service.QueryService;
import com.ARYD.MemoryDB.query.QueryEngine;
import com.ARYD.entity.QueryResult;
import com.ARYD.network.ClusterConfig;
import com.ARYD.network.ClusterManager;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * API REST for SQL query execution
 */
@RestController
@RequestMapping("/api/query")
@RequiredArgsConstructor
@Slf4j
public class QueryApi {
    private final QueryService queryService;
    private final QueryEngine queryEngine;
    private final ClusterManager clusterManager;
    private final ObjectMapper objectMapper = new ObjectMapper();
    
    /**
     * Execute a distributed SQL query across all nodes in the cluster
     */
    @PostMapping
    public ResponseEntity<Map<String, Object>> executeQuery(@RequestBody String sql) {
        long startTime = System.currentTimeMillis();
        
        try {
            if (sql == null || sql.trim().isEmpty()) {
                Map<String, Object> errorResponse = new HashMap<>();
                errorResponse.put("error", "SQL query is required");
                return ResponseEntity.badRequest().body(errorResponse);
            }
            
            log.info("Executing SQL query: {}", sql);
            
            try {
                // Execute the query locally
                List<Map<String, Object>> localResults = queryService.executeQuery(sql);
                
                // If we're in cluster mode, broadcast the query to other nodes
                if (ClusterConfig.getInstance().isInitialized() && ClusterConfig.getInstance().getAllNodes().size() > 1) {
                    try {
                        // Prepare remote query parameters
                        String requestBody = "{\"sql\":\"" + sql.replace("\"", "\\\"") + "\"}";
                        
                        // Send the query to other nodes
                        CompletableFuture<List<HttpResponse<String>>> broadcastFuture = 
                            clusterManager.broadcast("/api/query/local", "POST", requestBody);
                        
                        // Wait for results with timeout (60 seconds default)
                        List<Map<String, Object>> allResults = new ArrayList<>(localResults);
                        
                        try {
                            List<HttpResponse<String>> responses = broadcastFuture.get();
                            
                            for (HttpResponse<String> httpResponse : responses) {
                                if (httpResponse.statusCode() == 200) {
                                    try {
                                        // Parse JSON response
                                        String responseBody = httpResponse.body();
                                        List<Map<String, Object>> remoteResults = parseRemoteResults(responseBody);
                                        
                                        if (remoteResults != null) {
                                            allResults.addAll(remoteResults);
                                        }
                                    } catch (Exception e) {
                                        log.warn("Error parsing remote result: {}", e.getMessage());
                                    }
                                } else {
                                    log.warn("A node returned an error code: {}", httpResponse.statusCode());
                                }
                            }
                        } catch (Exception e) {
                            log.error("Error executing distributed query", e);
                        }
                        
                        // Return the combined results
                        Map<String, Object> response = new HashMap<>();
                        response.put("sql", sql);
                        response.put("results", allResults);
                        response.put("count", allResults.size());
                        response.put("distributed", true);
                        response.put("nodeCount", ClusterConfig.getInstance().getAllNodes().size());
                        response.put("executionTimeMs", System.currentTimeMillis() - startTime);
                        
                        return ResponseEntity.ok(response);
                    } catch (Exception e) {
                        log.error("Error in distributed query execution", e);
                        // Fall back to local results only
                        Map<String, Object> response = new HashMap<>();
                        response.put("sql", sql);
                        response.put("results", localResults);
                        response.put("count", localResults.size());
                        response.put("distributed", false);
                        response.put("error", "Failed to execute distributed query: " + e.getMessage());
                        response.put("executionTimeMs", System.currentTimeMillis() - startTime);
                        
                        return ResponseEntity.ok(response);
                    }
                } else {
                    // Standalone mode - just return local results
                    Map<String, Object> response = new HashMap<>();
                    response.put("sql", sql);
                    response.put("results", localResults);
                    response.put("count", localResults.size());
                    response.put("distributed", false);
                    response.put("executionTimeMs", System.currentTimeMillis() - startTime);
                    
                    return ResponseEntity.ok(response);
                }
            } catch (IllegalArgumentException e) {
                // Specific handling for table not found or other SQL parsing errors
                log.error("SQL query error: {}", e.getMessage());
                Map<String, Object> errorResponse = new HashMap<>();
                errorResponse.put("error", "SQL query error");
                errorResponse.put("message", e.getMessage());
                errorResponse.put("sql", sql);
                errorResponse.put("executionTimeMs", System.currentTimeMillis() - startTime);
                return ResponseEntity.badRequest().body(errorResponse);
            }
        } catch (Exception e) {
            log.error("Error executing SQL query", e);
            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("error", "Error executing SQL query");
            errorResponse.put("message", e.getMessage());
            errorResponse.put("sql", sql);
            errorResponse.put("executionTimeMs", System.currentTimeMillis() - startTime);
            return ResponseEntity.badRequest().body(errorResponse);
        }
    }
    
    /**
     * Endpoint for local query execution (used for internal communication)
     */
    @PostMapping("/local")
    public ResponseEntity<List<Map<String, Object>>> executeLocalQuery(
            @RequestBody Map<String, String> queryRequest,
            @RequestHeader(value = "X-Internal-Request", required = false) String internalRequest,
            @RequestHeader(value = "X-Source-Node", required = false) String sourceNode) {
        
        // Validate that this is an internal request
        if (!"true".equals(internalRequest)) {
            log.warn("Unauthorized attempt to access internal API");
            return ResponseEntity.status(403).build();
        }
        
        try {
            String sql = queryRequest.get("sql");
            
            if (sql == null || sql.trim().isEmpty()) {
                return ResponseEntity.badRequest().build();
            }
            
            log.info("Executing local SQL query from node {}: {}", sourceNode, sql);
            
            // Execute the query locally
            List<Map<String, Object>> results = queryService.executeQuery(sql);
            
            return ResponseEntity.ok(results);
        } catch (Exception e) {
            log.error("Error executing local SQL query", e);
            return ResponseEntity.internalServerError().build();
        }
    }
    
    /**
     * Utility to parse remote query results
     */
    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> parseRemoteResults(String json) {
        try {
            // In a real implementation, use Jackson or GSON properly
            // This is a basic implementation using ObjectMapper
            return objectMapper.readValue(json, List.class);
        } catch (Exception e) {
            log.error("Error parsing remote results JSON", e);
            return new ArrayList<>();
        }
    }
} 