package com.ARYD.network;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.concurrent.CompletableFuture;
import java.util.Optional;

/**
 * Représente un nœud de serveur dans le cluster ARYD
 */
public class ServerNode {
    
    private String name;
    private String address;
    private int port;
    private String status;
    private long lastHeartbeat;
    
    /**
     * Constructeur pour un nœud de serveur
     */
    public ServerNode(String name, String address, int port) {
        this.name = name;
        this.address = address;
        this.port = port;
        this.status = "ACTIVE";
        this.lastHeartbeat = System.currentTimeMillis();
    }
    
    /**
     * Obtient le nom du nœud
     */
    public String getName() {
        return name;
    }
    
    /**
     * Définit le nom du nœud
     */
    public void setName(String name) {
        this.name = name;
    }
    
    /**
     * Obtient l'adresse du nœud
     */
    public String getAddress() {
        return address;
    }
    
    /**
     * Définit l'adresse du nœud
     */
    public void setAddress(String address) {
        this.address = address;
    }
    
    /**
     * Obtient le port du nœud
     */
    public int getPort() {
        return port;
    }
    
    /**
     * Définit le port du nœud
     */
    public void setPort(int port) {
        this.port = port;
    }
    
    /**
     * Obtient le statut du nœud
     */
    public String getStatus() {
        return status;
    }
    
    /**
     * Définit le statut du nœud
     */
    public void setStatus(String status) {
        this.status = status;
    }
    
    /**
     * Vérifie si le nœud est actif
     * @return true si le statut du nœud est "ACTIVE", false sinon
     */
    public boolean isActive() {
        return "ACTIVE".equals(this.status);
    }
    
    /**
     * Obtient la dernière pulsation du nœud
     */
    public long getLastHeartbeat() {
        return lastHeartbeat;
    }
    
    /**
     * Met à jour la pulsation du nœud
     */
    public void updateHeartbeat() {
        this.lastHeartbeat = System.currentTimeMillis();
    }
    
    /**
     * Obtient l'URL complète du nœud
     */
    public String getBaseUrl() {
        return "http://" + address + ":" + port;
    }
    
    /**
     * Envoie une requête à ce nœud
     */
    public CompletableFuture<HttpResponse<String>> sendRequest(String endpoint, String method) {
        return sendRequest(endpoint, method, null);
    }
    
    /**
     * Envoie une requête avec un corps à ce nœud
     */
    public CompletableFuture<HttpResponse<String>> sendRequest(String endpoint, String method, String body) {
        HttpClient client = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .followRedirects(HttpClient.Redirect.ALWAYS)
                .build();
        
        HttpRequest.Builder requestBuilder = HttpRequest.newBuilder()
                .uri(URI.create(getBaseUrl() + endpoint))
                .headers("Content-Type", "application/json", 
                        "InternalToken", ClusterConfig.getInstance().getInternalToken());
        
        if (body != null && !body.isEmpty()) {
            requestBuilder.method(method, HttpRequest.BodyPublishers.ofString(body));
        } else {
            requestBuilder.method(method, HttpRequest.BodyPublishers.noBody());
        }
        
        HttpRequest request = requestBuilder.build();
        
        return client.sendAsync(request, HttpResponse.BodyHandlers.ofString());
    }
    
    /**
     * Vérifie si le nœud est en ligne
     */
    public CompletableFuture<Boolean> checkHealth() {
        return sendRequest("/api/health", "GET")
                .thenApply(response -> {
                    boolean isHealthy = response.statusCode() == 200;
                    this.status = isHealthy ? "ACTIVE" : "INACTIVE";
                    this.lastHeartbeat = System.currentTimeMillis();
                    return isHealthy;
                })
                .exceptionally(e -> {
                    this.status = "INACTIVE";
                    this.lastHeartbeat = System.currentTimeMillis();
                    return false;
                });
    }
    
    /**
     * Vérifie que la requête contient un secret valide
     */
    private boolean isValidRequest(java.net.http.HttpHeaders headers) {
        Optional<String> secret = headers.firstValue("X-Internal-Token");
        return secret.isPresent() && secret.get().equals(ClusterConfig.getInstance().getInternalToken());
    }
    
    @Override
    public String toString() {
        return "ServerNode{" +
                "name='" + name + '\'' +
                ", address='" + address + '\'' +
                ", port=" + port +
                ", status='" + status + '\'' +
                '}';
    }
}