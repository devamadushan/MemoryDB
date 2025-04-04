package com.ARYD.exception;

import javax.ws.rs.core.Response;

/**
 * Exception de base pour toutes les exceptions spécifiques à ARYD
 */
public class ARYDException extends RuntimeException {
    
    private final String errorCode;
    private final int httpStatus;
    
    /**
     * Crée une nouvelle exception ARYD
     * 
     * @param message Message d'erreur
     * @param errorCode Code d'erreur spécifique 
     * @param httpStatus Code de statut HTTP (par défaut 500)
     */
    public ARYDException(String message, String errorCode, int httpStatus) {
        super(message);
        this.errorCode = errorCode;
        this.httpStatus = httpStatus;
    }
    
    /**
     * Crée une nouvelle exception ARYD avec une cause
     *
     * @param message Message d'erreur
     * @param cause Exception qui a causé cette exception
     * @param errorCode Code d'erreur spécifique
     * @param httpStatus Code de statut HTTP
     */
    public ARYDException(String message, Throwable cause, String errorCode, int httpStatus) {
        super(message, cause);
        this.errorCode = errorCode;
        this.httpStatus = httpStatus;
    }
    
    /**
     * Crée une nouvelle exception ARYD avec un statut HTTP 500
     * 
     * @param message Message d'erreur
     * @param errorCode Code d'erreur spécifique
     */
    public ARYDException(String message, String errorCode) {
        this(message, errorCode, 500);
    }

    /**
     * Crée une nouvelle exception ARYD avec un statut HTTP 500 et une cause
     * 
     * @param message Message d'erreur
     * @param cause Exception qui a causé cette exception
     * @param errorCode Code d'erreur spécifique
     */
    public ARYDException(String message, Throwable cause, String errorCode) {
        this(message, cause, errorCode, 500);
    }

    /**
     * Constructeur avec message d'erreur, code d'erreur et statut HTTP
     */
    public ARYDException(String message, String errorCode, Response.Status httpStatus) {
        super(message);
        this.errorCode = errorCode;
        this.httpStatus = httpStatus.getStatusCode();
    }

    /**
     * Retourne le code d'erreur spécifique
     */
    public String getErrorCode() {
        return errorCode;
    }

    /**
     * Retourne le code de statut HTTP associé
     */
    public int getHttpStatus() {
        return httpStatus;
    }
} 