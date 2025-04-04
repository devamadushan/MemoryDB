package com.ARYD.core.storage;

import java.io.Serializable;

/**
 * Définition d'une colonne dans une table
 * Contient les informations sur le nom, le type, la nullabilité et l'indexation de la colonne
 */
public class ColumnDefinition implements Serializable {
    
    private static final long serialVersionUID = 1L;
    
    private final String name;
    private final String typeName;
    private final boolean nullable;
    private final boolean indexed;
    private String description;
    private String format;
    private String defaultValue;
    
    /**
     * Constructeur complet
     */
    public ColumnDefinition(String name, String typeName, boolean nullable, boolean indexed) {
        this.name = name;
        this.typeName = typeName;
        this.nullable = nullable;
        this.indexed = indexed;
    }
    
    /**
     * Constructeur avec valeurs par défaut (non-indexé)
     */
    public ColumnDefinition(String name, String typeName, boolean nullable) {
        this(name, typeName, nullable, false);
    }
    
    /**
     * Constructeur pour colonnes non-nullable et non-indexées
     */
    public ColumnDefinition(String name, String typeName) {
        this(name, typeName, false, false);
    }
    
    // Getters
    
    public String getName() {
        return name;
    }
    
    public String getTypeName() {
        return typeName;
    }
    
    public boolean isNullable() {
        return nullable;
    }
    
    public boolean isIndexed() {
        return indexed;
    }
    
    public String getDescription() {
        return description;
    }
    
    public String getFormat() {
        return format;
    }
    
    public String getDefaultValue() {
        return defaultValue;
    }
    
    // Setters pour les attributs optionnels
    
    public void setDescription(String description) {
        this.description = description;
    }
    
    public void setFormat(String format) {
        this.format = format;
    }
    
    public void setDefaultValue(String defaultValue) {
        this.defaultValue = defaultValue;
    }
    
    // Builder pattern pour la construction fluide
    
    public static Builder builder(String name, String typeName) {
        return new Builder(name, typeName);
    }
    
    public static class Builder {
        private final String name;
        private final String typeName;
        private boolean nullable = false;
        private boolean indexed = false;
        private String description;
        private String format;
        private String defaultValue;
        
        public Builder(String name, String typeName) {
            this.name = name;
            this.typeName = typeName;
        }
        
        public Builder nullable() {
            this.nullable = true;
            return this;
        }
        
        public Builder indexed() {
            this.indexed = true;
            return this;
        }
        
        public Builder description(String description) {
            this.description = description;
            return this;
        }
        
        public Builder format(String format) {
            this.format = format;
            return this;
        }
        
        public Builder defaultValue(String defaultValue) {
            this.defaultValue = defaultValue;
            return this;
        }
        
        public ColumnDefinition build() {
            ColumnDefinition col = new ColumnDefinition(name, typeName, nullable, indexed);
            if (description != null) col.setDescription(description);
            if (format != null) col.setFormat(format);
            if (defaultValue != null) col.setDefaultValue(defaultValue);
            return col;
        }
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(name).append(" (").append(typeName);
        if (nullable) sb.append(", NULLABLE");
        if (indexed) sb.append(", INDEXED");
        if (description != null) sb.append(", '").append(description).append("'");
        sb.append(")");
        return sb.toString();
    }
} 