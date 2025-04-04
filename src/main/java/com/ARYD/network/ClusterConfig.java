package com.ARYD.network;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import jakarta.annotation.PostConstruct;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

/**
 * Configuration for the cluster
 */
@Component
@Slf4j
public class ClusterConfig {
    private static ClusterConfig instance;
    
    private String currentNodeName;
    private String currentNodeAddress;
    private int currentNodePort;
    private boolean initialized = false;
    
    // List of all nodes in the cluster
    private final CopyOnWriteArrayList<ServerNode> allNodes = new CopyOnWriteArrayList<>();
    
    // Internal token to secure node-to-node communications
    private final String internalToken = UUID.randomUUID().toString();
    
    public ClusterConfig() {
        // Empty constructor for Spring
    }
    
    @PostConstruct
    public void init() {
        instance = this;
        
        // Try to get server ID from system property
        String serverId = System.getProperty("server.id");
        if (serverId != null) {
            this.currentNodeName = "node" + serverId;
            this.currentNodePort = 8080 + Integer.parseInt(serverId) - 1;
            this.currentNodeAddress = "localhost";
            log.info("Using server ID from system property: {}, port: {}", serverId, currentNodePort);
        }
    }
    
    /**
     * Get the singleton instance
     */
    public static ClusterConfig getInstance() {
        return instance;
    }
    
    /**
     * Initialize the current node
     */
    public void initializeNode(String nodeName, String nodeAddress, int nodePort) {
        this.currentNodeName = nodeName;
        this.currentNodeAddress = nodeAddress;
        this.currentNodePort = nodePort;
        this.initialized = true;
        
        // Create and add the current node
        ServerNode currentNode = new ServerNode(nodeName, nodeAddress, nodePort);
        addNode(currentNode);
        
        log.info("Initialized node: {}, address: {}:{}", nodeName, nodeAddress, nodePort);
    }
    
    /**
     * Add a node to the cluster
     */
    public void addNode(ServerNode node) {
        // Remove if exists (to avoid duplicates)
        allNodes.removeIf(n -> n.getName().equals(node.getName()));
        
        // Add the new node
        allNodes.add(node);
        log.info("Added node to cluster: {}", node.getName());
    }
    
    /**
     * Remove a node from the cluster
     */
    public boolean removeNode(String nodeName) {
        boolean removed = allNodes.removeIf(node -> node.getName().equals(nodeName));
        if (removed) {
            log.info("Removed node from cluster: {}", nodeName);
        } else {
            log.warn("Node not found for removal: {}", nodeName);
        }
        return removed;
    }
    
    /**
     * Get all nodes in the cluster
     */
    public List<ServerNode> getAllNodes() {
        return new ArrayList<>(allNodes);
    }
    
    /**
     * Get all nodes except the current one
     */
    public List<ServerNode> getOtherNodes() {
        return allNodes.stream()
                .filter(node -> !node.getName().equals(currentNodeName))
                .collect(Collectors.toList());
    }
    
    /**
     * Get the current node
     */
    public ServerNode getCurrentNode() {
        return new ServerNode(currentNodeName, currentNodeAddress, currentNodePort);
    }
    
    /**
     * Check if a node is the current node
     */
    public boolean isCurrentNode(String nodeName) {
        return currentNodeName != null && currentNodeName.equals(nodeName);
    }
    
    /**
     * Get the current node name
     */
    public String getCurrentNodeName() {
        return currentNodeName;
    }
    
    /**
     * Check if the cluster is initialized
     */
    public boolean isInitialized() {
        return initialized;
    }
    
    /**
     * Check if a token is valid for internal communication
     */
    public boolean isValidToken(String token) {
        return internalToken.equals(token);
    }
    
    /**
     * Get the internal token for node-to-node communication
     */
    public String getInternalToken() {
        return internalToken;
    }
    
    /**
     * Get a node by name
     */
    public ServerNode getNodeByName(String nodeName) {
        return allNodes.stream()
                .filter(node -> node.getName().equals(nodeName))
                .findFirst()
                .orElse(null);
    }
} 