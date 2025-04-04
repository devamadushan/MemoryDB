package com.ARYD.MemoryDB.types;

import java.util.HashMap;
import java.util.Map;

/**
 * Factory pour créer des instances de DataType en fonction de leur nom
 */
public class DataTypeFactory {
    
    private static final Map<String, DataType> typeInstanceCache = new HashMap<>();
    
    // Empêcher l'instanciation
    private DataTypeFactory() {}
    
    /**
     * Crée ou récupère une instance de DataType en fonction du nom du type
     * @param typeName Nom du type (insensible à la casse)
     * @return Instance de DataType
     * @throws IllegalArgumentException si le type est inconnu
     */
    public static DataType getType(String typeName) {
        if (typeName == null || typeName.isEmpty()) {
            throw new IllegalArgumentException("Le nom du type ne peut pas être null ou vide");
        }
        
        // Normaliser le nom du type
        String normalizedName = typeName.toUpperCase();
        
        // Vérifier si le type est déjà dans le cache
        if (typeInstanceCache.containsKey(normalizedName)) {
            return typeInstanceCache.get(normalizedName);
        }
        
        // Créer une nouvelle instance
        DataType type = createType(normalizedName);
        typeInstanceCache.put(normalizedName, type);
        return type;
    }
    
    /**
     * Crée une nouvelle instance de DataType
     * @param normalizedName Nom normalisé du type
     * @return Instance de DataType
     * @throws IllegalArgumentException si le type est inconnu
     */
    private static DataType createType(String normalizedName) {
        switch (normalizedName) {
            case "DOUBLE":
                return new DoubleType();
            case "LONG":
                return new LongType();
            case "INT":
            case "INTEGER":
                return new LongType(); // On utilise LongType pour int aussi
            case "STRING":
                return new StringType();
            case "VARCHAR":
                return new StringType(true, 0);
            case "CHAR":
                return new StringType(false, 255);
            // Ajouter d'autres types au besoin
            default:
                throw new IllegalArgumentException("Type inconnu: " + normalizedName);
        }
    }
    
    /**
     * Retourne le type Java associé à un nom de type SQL
     * @param sqlType Nom du type SQL
     * @return Classe Java correspondante
     */
    public static Class<?> getJavaType(String sqlType) {
        return getType(sqlType).getAssociatedClassType();
    }
    
    /**
     * Nettoie le cache
     */
    public static void clearCache() {
        typeInstanceCache.clear();
    }
} 
 