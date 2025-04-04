package com.ARYD.MemoryDB.index;

import com.ARYD.MemoryDB.query.Operator;
import com.ARYD.MemoryDB.query.Predicate;
import com.ARYD.MemoryDB.storage.Table;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class HashIndex implements Index {
    private final Table table;
    private final String[] indexedColumns;
    private final Map<Object, List<Integer>> valueToPositions;
    private final Set<Operator> compatibleOperators;

    public HashIndex(Table table, String[] indexedColumns) {
        this.table = table;
        this.indexedColumns = indexedColumns;
        this.valueToPositions = new ConcurrentHashMap<>();
        this.compatibleOperators = new HashSet<>(Arrays.asList(
            Operator.EQUALS,
            Operator.NOT_EQUALS,
            Operator.IN
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
        List<Integer> positions = valueToPositions.get(value);
        
        if (positions == null) {
            return new int[0];
        }

        if (predicate.getOperator() == Operator.NOT_EQUALS) {
            // Pour NOT_EQUALS, on retourne toutes les positions sauf celles de la valeur
            Set<Integer> allPositions = new HashSet<>();
            for (List<Integer> posList : valueToPositions.values()) {
                allPositions.addAll(posList);
            }
            allPositions.removeAll(positions);
            return allPositions.stream().mapToInt(Integer::intValue).toArray();
        }

        return positions.stream().mapToInt(Integer::intValue).toArray();
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
            log.info("HashIndex refreshed with {} unique values", valueToPositions.size());
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
 