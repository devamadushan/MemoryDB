package com.ARYD.core.storage;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Représente une requête à exécuter sur une table
 */
public class Query implements Serializable {
    
    private static final long serialVersionUID = 1L;
    
    // Types d'opérations
    public enum Operation {
        EQUALS, NOT_EQUALS, GREATER_THAN, LESS_THAN, GREATER_EQUALS, LESS_EQUALS,
        LIKE, CONTAINS, STARTS_WITH, ENDS_WITH, 
        IS_NULL, IS_NOT_NULL, 
        IN, NOT_IN, BETWEEN
    }
    
    // Opérateurs logiques
    public enum LogicalOperator {
        AND, OR
    }
    
    // Directions de tri
    public enum SortDirection {
        ASC, DESC
    }
    
    // Attributs de la requête
    private final List<String> selectColumns;
    private final List<Condition> conditions;
    private final List<SortCriteria> sortCriteria;
    private int limit = -1;
    private int offset = 0;
    private boolean distinct = false;
    private boolean returnRowCount = false;
    
    /**
     * Constructeur privé
     */
    private Query() {
        this.selectColumns = new ArrayList<>();
        this.conditions = new ArrayList<>();
        this.sortCriteria = new ArrayList<>();
    }
    
    // Builder pour construire une requête
    public static Builder select(String... columns) {
        return new Builder(columns);
    }
    
    public static Builder selectAll() {
        return new Builder();
    }
    
    public static Builder count() {
        Builder builder = new Builder();
        builder.query.returnRowCount = true;
        return builder;
    }
    
    // Getters
    
    public List<String> getSelectColumns() {
        return selectColumns;
    }
    
    public boolean isSelectAll() {
        return selectColumns.isEmpty();
    }
    
    public List<Condition> getConditions() {
        return conditions;
    }
    
    public List<SortCriteria> getSortCriteria() {
        return sortCriteria;
    }
    
    public int getLimit() {
        return limit;
    }
    
    public int getOffset() {
        return offset;
    }
    
    public boolean isDistinct() {
        return distinct;
    }
    
    public boolean isReturnRowCount() {
        return returnRowCount;
    }
    
    // Classe interne pour représenter une condition
    public static class Condition implements Serializable {
        private static final long serialVersionUID = 1L;
        
        private final String columnName;
        private final Operation operation;
        private final Object value;
        private final Object secondValue; // Utilisé pour BETWEEN
        private final LogicalOperator nextOperator;
        
        public Condition(String columnName, Operation operation, Object value, Object secondValue, LogicalOperator nextOperator) {
            this.columnName = columnName;
            this.operation = operation;
            this.value = value;
            this.secondValue = secondValue;
            this.nextOperator = nextOperator;
        }
        
        public String getColumnName() {
            return columnName;
        }
        
        public Operation getOperation() {
            return operation;
        }
        
        public Object getValue() {
            return value;
        }
        
        public Object getSecondValue() {
            return secondValue;
        }
        
        public LogicalOperator getNextOperator() {
            return nextOperator;
        }
        
        @Override
        public String toString() {
            switch (operation) {
                case IS_NULL:
                    return columnName + " IS NULL";
                case IS_NOT_NULL:
                    return columnName + " IS NOT NULL";
                case BETWEEN:
                    return columnName + " BETWEEN " + value + " AND " + secondValue;
                case IN:
                    return columnName + " IN " + value;
                case NOT_IN:
                    return columnName + " NOT IN " + value;
                default:
                    String op = operation.toString().replace("_", " ");
                    return columnName + " " + op + " " + value;
            }
        }
    }
    
    // Classe interne pour représenter un critère de tri
    public static class SortCriteria implements Serializable {
        private static final long serialVersionUID = 1L;
        
        private final String columnName;
        private final SortDirection direction;
        
        public SortCriteria(String columnName, SortDirection direction) {
            this.columnName = columnName;
            this.direction = direction;
        }
        
        public String getColumnName() {
            return columnName;
        }
        
        public SortDirection getDirection() {
            return direction;
        }
    }
    
    // Builder pour construire une requête de façon fluide
    public static class Builder {
        private final Query query;
        private LogicalOperator nextOperator = LogicalOperator.AND;
        
        private Builder(String... columns) {
            query = new Query();
            if (columns != null && columns.length > 0) {
                query.selectColumns.addAll(Arrays.asList(columns));
            }
        }
        
        // Méthodes pour ajouter des conditions
        
        public Builder where(String columnName, Operation operation, Object value) {
            query.conditions.add(new Condition(columnName, operation, value, null, nextOperator));
            nextOperator = LogicalOperator.AND;
            return this;
        }
        
        public Builder where(String columnName, String operation, Object value) {
            return where(columnName, parseOperation(operation), value);
        }
        
        public Builder andWhere(String columnName, Operation operation, Object value) {
            nextOperator = LogicalOperator.AND;
            return where(columnName, operation, value);
        }
        
        public Builder orWhere(String columnName, Operation operation, Object value) {
            nextOperator = LogicalOperator.OR;
            return where(columnName, operation, value);
        }
        
        public Builder isNull(String columnName) {
            query.conditions.add(new Condition(columnName, Operation.IS_NULL, null, null, nextOperator));
            nextOperator = LogicalOperator.AND;
            return this;
        }
        
        public Builder isNotNull(String columnName) {
            query.conditions.add(new Condition(columnName, Operation.IS_NOT_NULL, null, null, nextOperator));
            nextOperator = LogicalOperator.AND;
            return this;
        }
        
        public Builder in(String columnName, List<?> values) {
            query.conditions.add(new Condition(columnName, Operation.IN, values, null, nextOperator));
            nextOperator = LogicalOperator.AND;
            return this;
        }
        
        public Builder notIn(String columnName, List<?> values) {
            query.conditions.add(new Condition(columnName, Operation.NOT_IN, values, null, nextOperator));
            nextOperator = LogicalOperator.AND;
            return this;
        }
        
        public Builder between(String columnName, Object min, Object max) {
            query.conditions.add(new Condition(columnName, Operation.BETWEEN, min, max, nextOperator));
            nextOperator = LogicalOperator.AND;
            return this;
        }
        
        // Méthodes pour le tri, les limites, etc.
        
        public Builder orderBy(String columnName) {
            return orderBy(columnName, SortDirection.ASC);
        }
        
        public Builder orderBy(String columnName, SortDirection direction) {
            query.sortCriteria.add(new SortCriteria(columnName, direction));
            return this;
        }
        
        public Builder orderBy(String columnName, String direction) {
            return orderBy(columnName, "desc".equalsIgnoreCase(direction) ? SortDirection.DESC : SortDirection.ASC);
        }
        
        public Builder limit(int limit) {
            query.limit = limit;
            return this;
        }
        
        public Builder offset(int offset) {
            query.offset = offset;
            return this;
        }
        
        public Builder distinct() {
            query.distinct = true;
            return this;
        }
        
        // Méthode finale pour construire la requête
        public Query build() {
            return query;
        }
        
        // Utilitaire pour parser les opérations à partir de chaînes
        private Operation parseOperation(String operation) {
            if (operation == null) {
                return Operation.EQUALS;
            }
            
            operation = operation.trim().toUpperCase();
            
            switch (operation) {
                case "=":
                case "==":
                case "EQ":
                case "EQUALS":
                    return Operation.EQUALS;
                case "!=":
                case "<>":
                case "NE":
                case "NOT_EQUALS":
                    return Operation.NOT_EQUALS;
                case ">":
                case "GT":
                case "GREATER_THAN":
                    return Operation.GREATER_THAN;
                case "<":
                case "LT":
                case "LESS_THAN":
                    return Operation.LESS_THAN;
                case ">=":
                case "GE":
                case "GREATER_EQUALS":
                    return Operation.GREATER_EQUALS;
                case "<=":
                case "LE":
                case "LESS_EQUALS":
                    return Operation.LESS_EQUALS;
                case "LIKE":
                case "CONTAINS":
                    return Operation.LIKE;
                case "STARTS_WITH":
                    return Operation.STARTS_WITH;
                case "ENDS_WITH":
                    return Operation.ENDS_WITH;
                default:
                    throw new IllegalArgumentException("Operation inconnue: " + operation);
            }
        }
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("SELECT ");
        
        if (returnRowCount) {
            sb.append("COUNT(*)");
        } else if (distinct) {
            sb.append("DISTINCT ");
            if (selectColumns.isEmpty()) {
                sb.append("*");
            } else {
                sb.append(String.join(", ", selectColumns));
            }
        } else {
            if (selectColumns.isEmpty()) {
                sb.append("*");
            } else {
                sb.append(String.join(", ", selectColumns));
            }
        }
        
        sb.append(" FROM table");
        
        if (!conditions.isEmpty()) {
            sb.append(" WHERE ");
            boolean first = true;
            for (Condition condition : conditions) {
                if (!first) {
                    sb.append(" ")
                      .append(condition.getNextOperator())
                      .append(" ");
                }
                sb.append(condition);
                first = false;
            }
        }
        
        if (!sortCriteria.isEmpty()) {
            sb.append(" ORDER BY ");
            boolean first = true;
            for (SortCriteria sc : sortCriteria) {
                if (!first) {
                    sb.append(", ");
                }
                sb.append(sc.getColumnName())
                  .append(" ")
                  .append(sc.getDirection());
                first = false;
            }
        }
        
        if (limit >= 0) {
            sb.append(" LIMIT ").append(limit);
        }
        
        if (offset > 0) {
            sb.append(" OFFSET ").append(offset);
        }
        
        return sb.toString();
    }
} 
 