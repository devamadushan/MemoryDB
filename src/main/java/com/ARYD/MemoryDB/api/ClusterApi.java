package com.ARYD.MemoryDB.api;

import com.ARYD.network.ClusterConfig;
import com.ARYD.network.ClusterManager;
import com.ARYD.network.ServerNode;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * API REST pour les opérations de gestion du cluster
 */
@RestController
@RequestMapping("/api/cluster")
@RequiredArgsConstructor
@Slf4j
public class ClusterApi {
    private final ClusterManager clusterManager;
    private final ClusterConfig clusterConfig;
    
    /**
     * Initialize the current node as the first node in the cluster
     */
    @PostMapping("/init")
    public ResponseEntity<Map<String, Object>> initializeNode(@RequestBody Map<String, Object> request) {
        try {
            String nodeName = (String) request.get("nodeName");
            String nodeAddress = (String) request.get("nodeAddress");
            int nodePort = ((Number) request.get("nodePort")).intValue();
            
            if (clusterConfig.isInitialized()) {
                Map<String, Object> response = new HashMap<>();
                response.put("error", "Node already initialized");
                response.put("currentNodeName", clusterConfig.getCurrentNodeName());
                return ResponseEntity.badRequest().body(response);
            }
            
            // Initialize the node
            clusterConfig.initializeNode(nodeName, nodeAddress, nodePort);
            
            Map<String, Object> response = new HashMap<>();
            response.put("nodeName", nodeName);
            response.put("nodeAddress", nodeAddress);
            response.put("nodePort", nodePort);
            response.put("status", "initialized");
            
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            log.error("Error initializing node", e);
            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("error", "Error initializing node: " + e.getMessage());
            return ResponseEntity.internalServerError().body(errorResponse);
        }
    }
    
    /**
     * Add a node to the cluster
     */
    @PostMapping("/nodes")
    public ResponseEntity<Map<String, Object>> addNode(@RequestBody Map<String, Object> request) {
        try {
            // Check if the cluster is initialized
            if (!clusterConfig.isInitialized()) {
                Map<String, Object> response = new HashMap<>();
                response.put("error", "Cluster not initialized. Initialize this node first.");
                return ResponseEntity.badRequest().body(response);
            }
            
            String nodeName = (String) request.get("nodeName");
            String nodeAddress = (String) request.get("nodeAddress");
            int nodePort = ((Number) request.get("nodePort")).intValue();
            
            // Create and add the node
            ServerNode node = new ServerNode(nodeName, nodeAddress, nodePort);
            clusterConfig.addNode(node);
            
            Map<String, Object> response = new HashMap<>();
            response.put("nodeName", nodeName);
            response.put("nodeAddress", nodeAddress);
            response.put("nodePort", nodePort);
            response.put("status", "added");
            response.put("clusterSize", clusterConfig.getAllNodes().size());
            
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            log.error("Error adding node to cluster", e);
            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("error", "Error adding node to cluster: " + e.getMessage());
            return ResponseEntity.internalServerError().body(errorResponse);
        }
    }
    
    /**
     * Remove a node from the cluster
     */
    @DeleteMapping("/nodes/{nodeName}")
    public ResponseEntity<Map<String, Object>> removeNode(@PathVariable String nodeName) {
        try {
            // Check if the cluster is initialized
            if (!clusterConfig.isInitialized()) {
                Map<String, Object> response = new HashMap<>();
                response.put("error", "Cluster not initialized");
                return ResponseEntity.badRequest().body(response);
            }
            
            // Check if trying to remove the current node
            if (clusterConfig.isCurrentNode(nodeName)) {
                Map<String, Object> response = new HashMap<>();
                response.put("error", "Cannot remove the current node");
                return ResponseEntity.badRequest().body(response);
            }
            
            // Remove the node
            boolean removed = clusterConfig.removeNode(nodeName);
            
            if (!removed) {
                Map<String, Object> response = new HashMap<>();
                response.put("error", "Node not found: " + nodeName);
                return ResponseEntity.notFound().build();
            }
            
            Map<String, Object> response = new HashMap<>();
            response.put("nodeName", nodeName);
            response.put("status", "removed");
            response.put("clusterSize", clusterConfig.getAllNodes().size());
            
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            log.error("Error removing node from cluster", e);
            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("error", "Error removing node from cluster: " + e.getMessage());
            return ResponseEntity.internalServerError().body(errorResponse);
        }
    }
    
    /**
     * Get all nodes in the cluster
     */
    @GetMapping("/nodes")
    public ResponseEntity<Map<String, Object>> getNodes() {
        try {
            List<ServerNode> nodes = clusterConfig.getAllNodes();
            
            List<Map<String, Object>> nodesList = nodes.stream()
                    .map(node -> {
                        Map<String, Object> nodeMap = new HashMap<>();
                        nodeMap.put("nodeName", node.getName());
                        nodeMap.put("nodeAddress", node.getAddress());
                        nodeMap.put("nodePort", node.getPort());
                        nodeMap.put("isCurrentNode", clusterConfig.isCurrentNode(node.getName()));
                        return nodeMap;
                    })
                    .collect(Collectors.toList());
            
            Map<String, Object> response = new HashMap<>();
            response.put("nodes", nodesList);
            response.put("count", nodes.size());
            response.put("currentNode", clusterConfig.getCurrentNodeName());
            
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            log.error("Error getting cluster nodes", e);
            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("error", "Error getting cluster nodes: " + e.getMessage());
            return ResponseEntity.internalServerError().body(errorResponse);
        }
    }
    
    /**
     * Check the health of the cluster
     */
    @GetMapping("/health")
    public ResponseEntity<Map<String, Object>> checkHealth() {
        try {
            Map<String, Object> response = new HashMap<>();
            response.put("status", "ok");
            response.put("nodeName", clusterConfig.getCurrentNodeName());
            response.put("nodeCount", clusterConfig.getAllNodes().size());
            response.put("timestamp", System.currentTimeMillis());
            
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            log.error("Error checking cluster health", e);
            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("error", "Error checking cluster health: " + e.getMessage());
            return ResponseEntity.internalServerError().body(errorResponse);
        }
    }
    
    /**
     * Rebalance data across the cluster
     */
    @PostMapping("/rebalance")
    public ResponseEntity<Map<String, Object>> rebalanceData(@RequestBody Map<String, Object> request) {
        try {
            List<String> tables = (List<String>) request.get("tables");
            String strategy = (String) request.getOrDefault("strategy", "ROUND_ROBIN");
            
            // Start rebalancing (this would be async in a real implementation)
            clusterManager.rebalanceData(tables, strategy);
            
            Map<String, Object> response = new HashMap<>();
            response.put("status", "rebalancing_started");
            response.put("tables", tables);
            response.put("strategy", strategy);
            
            return ResponseEntity.accepted().body(response);
        } catch (Exception e) {
            log.error("Error rebalancing data", e);
            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("error", "Error rebalancing data: " + e.getMessage());
            return ResponseEntity.internalServerError().body(errorResponse);
        }
    }
    
    /**
     * Check data distribution for a specific table
     */
    @GetMapping("/distribution/{tableName}")
    public ResponseEntity<Map<String, Object>> checkDistribution(@PathVariable String tableName) {
        try {
            // Get distribution statistics from all nodes
            Map<String, Object> response = new HashMap<>();
            response.put("tableName", tableName);
            response.put("status", "not_implemented");
            response.put("message", "Distribution statistics will be implemented in a future release");
            
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            log.error("Error checking data distribution", e);
            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("error", "Error checking data distribution: " + e.getMessage());
            return ResponseEntity.internalServerError().body(errorResponse);
        }
    }
    
    /**
     * Get statistics for a specific node
     */
    @GetMapping("/nodes/{nodeName}/stats")
    public ResponseEntity<Map<String, Object>> getNodeStats(@PathVariable String nodeName) {
        try {
            // Get node statistics
            Map<String, Object> response = new HashMap<>();
            response.put("nodeName", nodeName);
            response.put("status", "not_implemented");
            response.put("message", "Node statistics will be implemented in a future release");
            
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            log.error("Error getting node statistics", e);
            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("error", "Error getting node statistics: " + e.getMessage());
            return ResponseEntity.internalServerError().body(errorResponse);
        }
    }
} 