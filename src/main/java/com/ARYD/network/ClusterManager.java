package com.ARYD.network;

import com.ARYD.entity.Record;
import com.ARYD.entity.TableCreationRequest;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Component;
import jakarta.annotation.PostConstruct;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Gère les communications du cluster
 */
@Component
@Slf4j
public class ClusterManager {
    private static ClusterManager instance;
    private final ClusterConfig config;
    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;
    private ScheduledExecutorService scheduler;
    
    // Health status of nodes
    private final ConcurrentHashMap<String, Boolean> nodeHealth = new ConcurrentHashMap<>();

    public ClusterManager(ClusterConfig config) {
        this.config = config;
        this.objectMapper = new ObjectMapper();
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .build();
        instance = this;
    }
    
    @PostConstruct
    public void init() {
        this.scheduler = Executors.newScheduledThreadPool(1);
        // Start health check scheduler
        startHealthChecks();
    }
    
    /**
     * Get singleton instance
     */
    public static ClusterManager getInstance() {
        return instance;
    }
    
    /**
     * Start periodic health checks for nodes
     */
    private void startHealthChecks() {
        scheduler.scheduleAtFixedRate(() -> {
            try {
                checkNodesHealth();
            } catch (Exception e) {
                log.error("Error during health check", e);
            }
        }, 10, 30, TimeUnit.SECONDS);
    }
    
    /**
     * Check health of all nodes in the cluster
     */
    private void checkNodesHealth() {
        List<ServerNode> nodes = config.getAllNodes();
        for (ServerNode node : nodes) {
            if (config.isCurrentNode(node.getName())) {
                // Skip self, we're obviously healthy
                nodeHealth.put(node.getName(), true);
                continue;
            }
            
            CompletableFuture.runAsync(() -> {
                try {
                    String url = node.getBaseUrl() + "/api/cluster/health";
                    HttpRequest request = HttpRequest.newBuilder()
                            .uri(URI.create(url))
                            .header("Accept", "application/json")
                            .header("X-Internal-Request", "true")
                            .timeout(Duration.ofSeconds(3))
                            .GET()
                            .build();
                    
                    HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
                    boolean isHealthy = response.statusCode() == 200;
                    
                    nodeHealth.put(node.getName(), isHealthy);
                    
                    if (!isHealthy) {
                        log.warn("Node {} is unhealthy", node.getName());
                    }
                } catch (Exception e) {
                    log.warn("Node {} is unreachable: {}", node.getName(), e.getMessage());
                    nodeHealth.put(node.getName(), false);
                }
            });
        }
    }
    
    /**
     * Get list of active (healthy) nodes
     */
    public List<ServerNode> getActiveNodes() {
        List<ServerNode> activeNodes = new ArrayList<>();
        List<ServerNode> allNodes = config.getAllNodes();
        
        for (ServerNode node : allNodes) {
            // If health status is unknown or true, consider it active
            Boolean isHealthy = nodeHealth.getOrDefault(node.getName(), true);
            if (isHealthy) {
                activeNodes.add(node);
            }
        }
        
        return activeNodes;
    }
    
    /**
     * Propagate table creation to all nodes in the cluster
     */
    public void propagateTableCreation(TableCreationRequest request, HttpHeaders headers) {
        List<ServerNode> nodes = config.getAllNodes();
        for (ServerNode node : nodes) {
            if (config.isCurrentNode(node.getName())) {
                // Skip self
                continue;
            }
            
            CompletableFuture.runAsync(() -> {
                try {
                    String url = node.getBaseUrl() + "/api/tables/create";
                    String jsonBody = objectMapper.writeValueAsString(request);
                    
                    HttpRequest httpRequest = HttpRequest.newBuilder()
                            .uri(URI.create(url))
                            .header("Content-Type", "application/json")
                            .header("Accept", "application/json")
                            .header("X-Internal-Request", "true")
                            .header("X-Source-Node", config.getCurrentNodeName())
                            .POST(HttpRequest.BodyPublishers.ofString(jsonBody))
                            .build();
                    
                    HttpResponse<String> response = httpClient.send(httpRequest, HttpResponse.BodyHandlers.ofString());
                    if (response.statusCode() != 200) {
                        log.error("Failed to propagate table creation to node {}: {} - {}", 
                                node.getName(), response.statusCode(), response.body());
                    } else {
                        log.info("Successfully propagated table creation to node {}", node.getName());
                    }
                } catch (Exception e) {
                    log.error("Error propagating table creation to node {}", node.getName(), e);
                }
            });
        }
    }
    
    /**
     * Send a record to a specific node
     */
    public boolean sendRecord(ServerNode node, Record record) {
        try {
            String url = node.getBaseUrl() + "/api/tables/record";
            String jsonBody = objectMapper.writeValueAsString(record);
            
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .header("Content-Type", "application/json")
                    .header("Accept", "application/json")
                    .header("X-Internal-Request", "true")
                    .header("X-Source-Node", config.getCurrentNodeName())
                    .POST(HttpRequest.BodyPublishers.ofString(jsonBody))
                    .build();
            
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            
            if (response.statusCode() != 200) {
                log.error("Failed to send record to node {}: {} - {}", 
                        node.getName(), response.statusCode(), response.body());
                return false;
            }
            
            return true;
        } catch (Exception e) {
            log.error("Error sending record to node {}", node.getName(), e);
            return false;
        }
    }
    
    /**
     * Broadcast a request to all active nodes
     */
    public CompletableFuture<List<HttpResponse<String>>> broadcast(String path, String method, String body) {
        List<ServerNode> nodes = getActiveNodes();
        List<CompletableFuture<HttpResponse<String>>> futures = new ArrayList<>();
        
        for (ServerNode node : nodes) {
            if (config.isCurrentNode(node.getName())) {
                // Skip self - we'll handle the local case separately
                continue;
            }
            
            CompletableFuture<HttpResponse<String>> future = CompletableFuture.supplyAsync(() -> {
                try {
                    String url = node.getBaseUrl() + path;
                    HttpRequest.Builder requestBuilder = HttpRequest.newBuilder()
                            .uri(URI.create(url))
                            .header("Accept", "application/json")
                            .header("X-Internal-Request", "true")
                            .header("X-Source-Node", config.getCurrentNodeName());
                    
                    HttpRequest request;
                    if ("GET".equalsIgnoreCase(method)) {
                        request = requestBuilder.GET().build();
                    } else if ("POST".equalsIgnoreCase(method)) {
                        requestBuilder.header("Content-Type", "application/json");
                        request = requestBuilder.POST(HttpRequest.BodyPublishers.ofString(body != null ? body : "{}")).build();
                    } else if ("DELETE".equalsIgnoreCase(method)) {
                        request = requestBuilder.DELETE().build();
                    } else {
                        throw new IllegalArgumentException("Unsupported HTTP method: " + method);
                    }
                    
                    return httpClient.send(request, HttpResponse.BodyHandlers.ofString());
                } catch (IOException | InterruptedException e) {
                    log.error("Error in broadcast to node {}: {}", node.getName(), e.getMessage());
                    throw new RuntimeException(e);
                }
            });
            
            futures.add(future);
        }
        
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .thenApply(v -> futures.stream()
                        .map(CompletableFuture::join)
                        .collect(ArrayList::new, ArrayList::add, ArrayList::addAll));
    }
    
    /**
     * Rebalance data across nodes
     */
    public void rebalanceData(List<String> tableNames, String strategy) {
        // Not implemented in this example - would redistribute data after node changes
        log.info("Rebalance request received for tables: {}, strategy: {}", tableNames, strategy);
    }
} 
 