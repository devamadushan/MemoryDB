package com.ARYD.core.types;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Définit les types de données supportés par le système et fournit
 * des méthodes pour convertir et valider les valeurs
 */
public final class DataType {
    
    // Types constants
    public static final String STRING = "STRING";
    public static final String INT = "INT";
    public static final String LONG = "LONG";
    public static final String FLOAT = "FLOAT";
    public static final String DOUBLE = "DOUBLE";
    public static final String BOOLEAN = "BOOLEAN";
    public static final String DATE = "DATE";
    public static final String TIMESTAMP = "TIMESTAMP";
    public static final String BINARY = "BINARY";
    public static final String DECIMAL = "DECIMAL";
    public static final String ARRAY = "ARRAY";
    public static final String MAP = "MAP";
    public static final String JSON = "JSON";
    
    // Mapping des types vers leurs classes Java
    private static final Map<String, Class<?>> TYPE_MAPPING = new HashMap<>();
    
    static {
        TYPE_MAPPING.put(STRING, String.class);
        TYPE_MAPPING.put(INT, Integer.class);
        TYPE_MAPPING.put(LONG, Long.class);
        TYPE_MAPPING.put(FLOAT, Float.class);
        TYPE_MAPPING.put(DOUBLE, Double.class);
        TYPE_MAPPING.put(BOOLEAN, Boolean.class);
        TYPE_MAPPING.put(DATE, LocalDate.class);
        TYPE_MAPPING.put(TIMESTAMP, LocalDateTime.class);
        TYPE_MAPPING.put(BINARY, byte[].class);
        TYPE_MAPPING.put(DECIMAL, BigDecimal.class);
        TYPE_MAPPING.put(ARRAY, Object[].class);
        TYPE_MAPPING.put(MAP, Map.class);
        TYPE_MAPPING.put(JSON, String.class);
    }
    
    // Constructeur privé pour empêcher l'instanciation
    private DataType() {
        throw new UnsupportedOperationException("Cette classe ne peut pas être instanciée");
    }
    
    /**
     * Vérifie si un type est valide
     */
    public static boolean isValidType(String typeName) {
        return TYPE_MAPPING.containsKey(typeName);
    }
    
    /**
     * Obtient la classe Java correspondant à un type
     */
    public static Class<?> getJavaType(String typeName) {
        if (!isValidType(typeName)) {
            throw new IllegalArgumentException("Type inconnu: " + typeName);
        }
        return TYPE_MAPPING.get(typeName);
    }
    
    /**
     * Détermine le type de données à partir d'une valeur
     */
    public static String inferType(Object value) {
        if (value == null) {
            return STRING; // Par défaut
        }
        
        if (value instanceof String) {
            String str = (String) value;
            
            // Essayer de détecter les dates et timestamps
            if (isDateString(str)) {
                return DATE;
            }
            
            if (isTimestampString(str)) {
                return TIMESTAMP;
            }
            
            // Essayer de détecter les booléens
            if (isBooleanString(str)) {
                return BOOLEAN;
            }
            
            // Essayer de détecter les nombres
            if (isIntegerString(str)) {
                long longValue = Long.parseLong(str);
                if (longValue >= Integer.MIN_VALUE && longValue <= Integer.MAX_VALUE) {
                    return INT;
                } else {
                    return LONG;
                }
            }
            
            if (isFloatingPointString(str)) {
                double doubleValue = Double.parseDouble(str);
                if (doubleValue >= Float.MIN_VALUE && doubleValue <= Float.MAX_VALUE) {
                    return FLOAT;
                } else {
                    return DOUBLE;
                }
            }
            
            return STRING;
        }
        
        if (value instanceof Integer) {
            return INT;
        }
        
        if (value instanceof Long) {
            return LONG;
        }
        
        if (value instanceof Float) {
            return FLOAT;
        }
        
        if (value instanceof Double) {
            return DOUBLE;
        }
        
        if (value instanceof Boolean) {
            return BOOLEAN;
        }
        
        if (value instanceof LocalDate) {
            return DATE;
        }
        
        if (value instanceof LocalDateTime || value instanceof Date || value instanceof Timestamp) {
            return TIMESTAMP;
        }
        
        if (value instanceof byte[]) {
            return BINARY;
        }
        
        if (value instanceof BigDecimal) {
            return DECIMAL;
        }
        
        if (value.getClass().isArray()) {
            return ARRAY;
        }
        
        if (value instanceof Map) {
            return MAP;
        }
        
        return STRING; // Par défaut
    }
    
    /**
     * Convertit une valeur au type spécifié
     */
    public static Object convertToType(Object value, String targetType) {
        if (value == null) {
            return null;
        }
        
        // Si la valeur est déjà du bon type, pas besoin de conversion
        if (value.getClass().equals(getJavaType(targetType))) {
            return value;
        }
        
        // Conversion depuis une chaîne
        if (value instanceof String) {
            String strValue = (String) value;
            
            switch (targetType) {
                case STRING:
                    return strValue;
                    
                case INT:
                    try {
                        return Integer.parseInt(strValue.trim());
                    } catch (NumberFormatException e) {
                        throw new IllegalArgumentException("Impossible de convertir en INT: " + strValue);
                    }
                    
                case LONG:
                    try {
                        return Long.parseLong(strValue.trim());
                    } catch (NumberFormatException e) {
                        throw new IllegalArgumentException("Impossible de convertir en LONG: " + strValue);
                    }
                    
                case FLOAT:
                    try {
                        return Float.parseFloat(strValue.trim());
                    } catch (NumberFormatException e) {
                        throw new IllegalArgumentException("Impossible de convertir en FLOAT: " + strValue);
                    }
                    
                case DOUBLE:
                    try {
                        return Double.parseDouble(strValue.trim());
                    } catch (NumberFormatException e) {
                        throw new IllegalArgumentException("Impossible de convertir en DOUBLE: " + strValue);
                    }
                    
                case BOOLEAN:
                    return parseBoolean(strValue);
                    
                case DATE:
                    try {
                        return LocalDate.parse(strValue);
                    } catch (DateTimeParseException e) {
                        try {
                            // Essayer avec d'autres formats
                            return LocalDate.parse(strValue, DateTimeFormatter.ISO_DATE);
                        } catch (DateTimeParseException e2) {
                            throw new IllegalArgumentException("Impossible de convertir en DATE: " + strValue);
                        }
                    }
                    
                case TIMESTAMP:
                    try {
                        return LocalDateTime.parse(strValue);
                    } catch (DateTimeParseException e) {
                        try {
                            // Essayer avec d'autres formats communs
                            return LocalDateTime.parse(strValue, DateTimeFormatter.ISO_DATE_TIME);
                        } catch (DateTimeParseException e2) {
                            throw new IllegalArgumentException("Impossible de convertir en TIMESTAMP: " + strValue);
                        }
                    }
                    
                case DECIMAL:
                    try {
                        return new BigDecimal(strValue.trim());
                    } catch (NumberFormatException e) {
                        throw new IllegalArgumentException("Impossible de convertir en DECIMAL: " + strValue);
                    }
                    
                default:
                    throw new IllegalArgumentException("Conversion non supportée de String vers " + targetType);
            }
        }
        
        // Conversion de Number vers les autres types numériques
        if (value instanceof Number) {
            Number numValue = (Number) value;
            
            switch (targetType) {
                case STRING:
                    return numValue.toString();
                    
                case INT:
                    return numValue.intValue();
                    
                case LONG:
                    return numValue.longValue();
                    
                case FLOAT:
                    return numValue.floatValue();
                    
                case DOUBLE:
                    return numValue.doubleValue();
                    
                case DECIMAL:
                    if (value instanceof BigDecimal) {
                        return value;
                    } else {
                        return BigDecimal.valueOf(numValue.doubleValue());
                    }
                    
                default:
                    throw new IllegalArgumentException("Conversion non supportée de Number vers " + targetType);
            }
        }
        
        // Conversion de Date/LocalDate/LocalDateTime vers les types temporels
        if (value instanceof LocalDate && targetType.equals(DATE)) {
            return value;
        }
        
        if (value instanceof LocalDate && targetType.equals(TIMESTAMP)) {
            return ((LocalDate) value).atStartOfDay();
        }
        
        if (value instanceof LocalDateTime && targetType.equals(TIMESTAMP)) {
            return value;
        }
        
        if (value instanceof LocalDateTime && targetType.equals(DATE)) {
            return ((LocalDateTime) value).toLocalDate();
        }
        
        if (value instanceof Date) {
            Timestamp ts = new Timestamp(((Date) value).getTime());
            if (targetType.equals(TIMESTAMP)) {
                return ts.toLocalDateTime();
            } else if (targetType.equals(DATE)) {
                return ts.toLocalDateTime().toLocalDate();
            } else if (targetType.equals(STRING)) {
                return ts.toString();
            }
        }
        
        // Conversion par défaut vers String
        if (targetType.equals(STRING)) {
            return value.toString();
        }
        
        throw new IllegalArgumentException("Conversion non supportée de " + value.getClass().getSimpleName() + " vers " + targetType);
    }
    
    // Utilitaires
    
    private static boolean isIntegerString(String str) {
        try {
            Long.parseLong(str.trim());
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }
    
    private static boolean isFloatingPointString(String str) {
        try {
            Double.parseDouble(str.trim());
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }
    
    private static boolean isBooleanString(String str) {
        String trimmed = str.trim().toLowerCase();
        return trimmed.equals("true") || trimmed.equals("false") || 
               trimmed.equals("yes") || trimmed.equals("no") ||
               trimmed.equals("1") || trimmed.equals("0") ||
               trimmed.equals("t") || trimmed.equals("f") ||
               trimmed.equals("y") || trimmed.equals("n");
    }
    
    private static boolean parseBoolean(String str) {
        String trimmed = str.trim().toLowerCase();
        return trimmed.equals("true") || trimmed.equals("yes") || 
               trimmed.equals("1") || trimmed.equals("t") || 
               trimmed.equals("y");
    }
    
    private static boolean isDateString(String str) {
        try {
            LocalDate.parse(str);
            return true;
        } catch (DateTimeParseException e) {
            try {
                LocalDate.parse(str, DateTimeFormatter.ISO_DATE);
                return true;
            } catch (DateTimeParseException e2) {
                return false;
            }
        }
    }
    
    private static boolean isTimestampString(String str) {
        try {
            LocalDateTime.parse(str);
            return true;
        } catch (DateTimeParseException e) {
            try {
                LocalDateTime.parse(str, DateTimeFormatter.ISO_DATE_TIME);
                return true;
            } catch (DateTimeParseException e2) {
                return false;
            }
        }
    }
} 