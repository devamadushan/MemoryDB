package com.ARYD.MemoryDB.util;

import com.ARYD.MemoryDB.types.DataType;

/**
 * Utility class to map between Parquet types and internal DataTypes
 */
public class DataTypeMapper {

    /**
     * Convert a Parquet type to our internal DataType
     * @param parquetType The Parquet type name
     * @return The corresponding internal DataType name
     */
    public static String fromParquetType(String parquetType) {
        switch (parquetType.toUpperCase()) {
            case "INT32":
            case "INT":
                return "INT";
            case "INT64":
            case "LONG":
                return "LONG";
            case "FLOAT":
                return "FLOAT";
            case "DOUBLE":
                return "DOUBLE";
            case "BOOLEAN":
                return "BOOLEAN";
            case "BINARY":
                return "STRING"; // BINARY in Parquet is often used for strings
            case "TIMESTAMP":
            case "TIMESTAMP_MICROS":
            case "TIMESTAMP_MILLIS":
                return "TIMESTAMP";
            case "DATE":
                return "DATE";
            case "DECIMAL":
                return "DOUBLE"; // Use double as a fallback for decimal
            default:
                return "STRING"; // Default to string for unknown types
        }
    }
} 