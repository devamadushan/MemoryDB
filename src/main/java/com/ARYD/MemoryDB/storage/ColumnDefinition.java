package com.ARYD.MemoryDB.storage;

import lombok.Data;
import lombok.RequiredArgsConstructor;

@Data
@RequiredArgsConstructor
public class ColumnDefinition {
    private final String name;
    private final String type;
    private final boolean nullable;
    private final boolean indexed;

    public static ColumnDefinition of(String name, String type) {
        return new ColumnDefinition(name, type, true, false);
    }

    public static ColumnDefinition of(String name, String type, boolean nullable) {
        return new ColumnDefinition(name, type, nullable, false);
    }

    public static ColumnDefinition of(String name, String type, boolean nullable, boolean indexed) {
        return new ColumnDefinition(name, type, nullable, indexed);
    }
} 
 