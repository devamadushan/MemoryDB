package com.ARYD.MemoryDB.query;

import lombok.Data;
import lombok.RequiredArgsConstructor;

@Data
@RequiredArgsConstructor
public class Predicate {
    private final String column;
    private final Operator operator;
    private final Object value;

    public static Predicate equals(String column, Object value) {
        return new Predicate(column, Operator.EQUALS, value);
    }

    public static Predicate notEquals(String column, Object value) {
        return new Predicate(column, Operator.NOT_EQUALS, value);
    }

    public static Predicate greater(String column, Object value) {
        return new Predicate(column, Operator.GREATER, value);
    }

    public static Predicate greaterEquals(String column, Object value) {
        return new Predicate(column, Operator.GREATER_EQUALS, value);
    }

    public static Predicate less(String column, Object value) {
        return new Predicate(column, Operator.LESS, value);
    }

    public static Predicate lessEquals(String column, Object value) {
        return new Predicate(column, Operator.LESS_EQUALS, value);
    }

    public static Predicate between(String column, Object min, Object max) {
        return new Predicate(column, Operator.BETWEEN, new Object[]{min, max});
    }

    public static Predicate in(String column, Object... values) {
        return new Predicate(column, Operator.IN, values);
    }

    public static Predicate like(String column, String pattern) {
        return new Predicate(column, Operator.LIKE, pattern);
    }

    public static Predicate isNull(String column) {
        return new Predicate(column, Operator.IS_NULL, null);
    }

    public static Predicate isNotNull(String column) {
        return new Predicate(column, Operator.IS_NOT_NULL, null);
    }
} 
 