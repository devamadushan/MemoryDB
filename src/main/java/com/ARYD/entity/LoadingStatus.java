package com.ARYD.entity;

import java.time.LocalDateTime;

/**
 * Suivi du statut de chargement d'un fichier
 */
public class LoadingStatus {
    private String id;
    private String filePath;
    private String status; // STARTED, LOADING, COMPLETED, FAILED
    private int rowCount;
    private int progress; // pourcentage (0-100)
    private String errorMessage;
    private LocalDateTime startTime;
    private LocalDateTime endTime;

    public LoadingStatus() {
        // Constructeur par défaut requis pour JAX-RS
    }

    public LoadingStatus(String id, String filePath, String status, int rowCount, int progress) {
        this.id = id;
        this.filePath = filePath;
        this.status = status;
        this.rowCount = rowCount;
        this.progress = progress;
        this.startTime = LocalDateTime.now();
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getFilePath() {
        return filePath;
    }

    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
        if ("COMPLETED".equals(status) || "FAILED".equals(status)) {
            this.endTime = LocalDateTime.now();
        }
    }

    public int getRowCount() {
        return rowCount;
    }

    public void setRowCount(int rowCount) {
        this.rowCount = rowCount;
    }

    public int getProgress() {
        return progress;
    }

    public void setProgress(int progress) {
        this.progress = Math.min(100, Math.max(0, progress));
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    public LocalDateTime getStartTime() {
        return startTime;
    }

    public void setStartTime(LocalDateTime startTime) {
        this.startTime = startTime;
    }

    public LocalDateTime getEndTime() {
        return endTime;
    }

    public void setEndTime(LocalDateTime endTime) {
        this.endTime = endTime;
    }

    public long getDurationSeconds() {
        if (endTime == null) {
            return LocalDateTime.now().toEpochSecond(java.time.ZoneOffset.UTC) - 
                   startTime.toEpochSecond(java.time.ZoneOffset.UTC);
        }
        return endTime.toEpochSecond(java.time.ZoneOffset.UTC) - 
               startTime.toEpochSecond(java.time.ZoneOffset.UTC);
    }
} 
 