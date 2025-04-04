package com.ARYD.MemoryDB.index;

import com.ARYD.MemoryDB.query.Operator;
import com.ARYD.MemoryDB.query.Predicate;
import com.ARYD.MemoryDB.storage.Table;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;

@Slf4j
public class TreeIndex implements Index {
    private final Table table;
    private final String[] indexedColumns;
    private final ConcurrentSkipListMap<Object, List<Integer>> valueToPositions;
    private final Set<Operator> compatibleOperators;

    public TreeIndex(Table table, String[] indexedColumns) {
        this.table = table;
        this.indexedColumns = indexedColumns;
        this.valueToPositions = new ConcurrentSkipListMap<>();
        this.compatibleOperators = new HashSet<>(Arrays.asList(
            Operator.EQUALS,
            Operator.NOT_EQUALS,
            Operator.GREATER,
            Operator.GREATER_EQUALS,
            Operator.LESS,
            Operator.LESS_EQUALS,
            Operator.BETWEEN
        ));
    }

    @Override
    public Table getTable() {
        return table;
    }

    @Override
    public String[] getIndexedColumns() {
        return indexedColumns;
    }

    @Override
    public boolean isOperatorCompatible(Operator op) {
        return compatibleOperators.contains(op);
    }

    @Override
    public Operator[] compatibleOperators() {
        return compatibleOperators.toArray(new Operator[0]);
    }

    @Override
    public int[] getIdsFromPredicate(Predicate predicate) throws IndexException {
        if (!canBeUsedWithPredicate(predicate)) {
            throw new IndexException("Index cannot be used with this predicate");
        }

        Object value = predicate.getValue();
        Set<Integer> result = new HashSet<>();

        switch (predicate.getOperator()) {
            case EQUALS:
                List<Integer> positions = valueToPositions.get(value);
                if (positions != null) {
                    result.addAll(positions);
                }
                break;

            case NOT_EQUALS:
                // On ajoute toutes les positions sauf celles de la valeur
                for (List<Integer> posList : valueToPositions.values()) {
                    result.addAll(posList);
                }
                List<Integer> excludedPositions = valueToPositions.get(value);
                if (excludedPositions != null) {
                    result.removeAll(excludedPositions);
                }
                break;

            case GREATER:
                valueToPositions.tailMap(value, false).values().forEach(result::addAll);
                break;

            case GREATER_EQUALS:
                valueToPositions.tailMap(value, true).values().forEach(result::addAll);
                break;

            case LESS:
                valueToPositions.headMap(value, false).values().forEach(result::addAll);
                break;

            case LESS_EQUALS:
                valueToPositions.headMap(value, true).values().forEach(result::addAll);
                break;

            case BETWEEN:
                Object[] values = (Object[]) value;
                if (values.length == 2) {
                    valueToPositions.subMap(values[0], true, values[1], true)
                            .values().forEach(result::addAll);
                }
                break;
        }

        return result.stream().mapToInt(Integer::intValue).toArray();
    }

    @Override
    public void refreshIndexWithColumnsData(boolean verbose) {
        valueToPositions.clear();
        int rowCount = table.getRowCount();
        
        for (int i = 0; i < rowCount; i++) {
            Object value = table.getValue(i, indexedColumns[0]);
            if (value != null) {
                valueToPositions.computeIfAbsent(value, k -> new ArrayList<>()).add(i);
            }
        }
        
        if (verbose) {
            log.info("TreeIndex refreshed with {} unique values", valueToPositions.size());
        }
    }

    @Override
    public void addValue(Object value, int position) throws IOException {
        if (value != null) {
            valueToPositions.computeIfAbsent(value, k -> new ArrayList<>()).add(position);
        }
    }

    @Override
    public void flushOnDisk() throws IOException {
        // Pour l'instant, on ne persiste pas sur le disque
        // On pourra implémenter la persistance plus tard
    }
} 
 