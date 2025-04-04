package com.ARYD.MemoryDB.types;

/**
 * Enumération des opérateurs supportés pour les requêtes et filtres
 */
public enum Operator {
    // Opérateurs standards
    EQUALS("="),
    GREATER(">"),
    LESS("<"),
    GREATER_OR_EQUALS(">="),
    LESS_OR_EQUALS("<="),
    NOT_EQUALS("!="),
    
    // Opérateurs composés
    BETWEEN("BETWEEN"),
    IN("IN"),
    
    // Opérateurs textuels
    LIKE("LIKE"),
    CONTAINS("CONTAINS"),
    STARTS_WITH("STARTS WITH"),
    ENDS_WITH("ENDS WITH"),
    
    // Opérateurs logiques
    AND("AND"),
    OR("OR"),
    NOT("NOT");
    
    private final String symbol;
    
    Operator(String symbol) {
        this.symbol = symbol;
    }
    
    public String getSymbol() {
        return symbol;
    }
    
    /**
     * Recherche un opérateur par son symbole
     */
    public static Operator fromSymbol(String symbol) {
        for (Operator op : values()) {
            if (op.symbol.equalsIgnoreCase(symbol)) {
                return op;
            }
        }
        throw new IllegalArgumentException("Opérateur inconnu: " + symbol);
    }
} 
 