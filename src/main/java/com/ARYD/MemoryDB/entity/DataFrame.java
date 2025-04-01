package com.ARYD.MemoryDB.entity;

import lombok.Getter;
import lombok.Setter;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class DataFrame {

    // Liste ordonnée des noms de colonnes (le schéma)
    @Getter
    private List<String> columnOrder = new ArrayList<>();

    // Pour chaque colonne, on stocke une ColumnVector qui encapsule un tableau préalloué
    @Getter
    private Map<String, ColumnVector<Object>> columns = new LinkedHashMap<>();

    // Gestion thread-safe du nombre de lignes insérées
    private AtomicInteger rowCount = new AtomicInteger(0);

    @Getter @Setter
    private String tableName;

    /**
     * Colonne en mémoire inspirée d'Apache Arrow.
     * Chaque colonne est un tableau contigu de valeurs.
     */
    public static class ColumnVector<T> {
        private T[] data;

        @SuppressWarnings("unchecked")
        public ColumnVector(int capacity) {
            data = (T[]) new Object[capacity];
        }

        public void set(int index, T value) {
            data[index] = value;
        }

        public T get(int index) {
            return data[index];
        }
    }

    /**
     * Initialisation des colonnes à partir d'une liste ordonnée de noms et d'une capacité préallouée.
     */
    public void initializeColumns(List<String> columnNames, int capacity) {
        this.columnOrder = new ArrayList<>(columnNames);
        for (String name : columnNames) {
            columns.put(name, new ColumnVector<>(capacity));
        }
    }

    /**
     * Insère une ligne dans la DataFrame à l'index spécifié.
     * La ligne est représentée par un tableau d'objets dont l'ordre correspond à columnOrder.
     */
    public void setRow(int index, Object[] row) {
        for (int i = 0; i < columnOrder.size(); i++) {
            String col = columnOrder.get(i);
            columns.get(col).set(index, row[i]);
        }
        rowCount.updateAndGet(current -> Math.max(current, index + 1));
    }

    /**
     * Retourne une ligne sous forme de Map (nom de colonne -> valeur).
     */
    public Map<String, Object> getRow(int index) {
        if (index < 0 || index >= rowCount.get()) {
            throw new IndexOutOfBoundsException("Index hors limites !");
        }
        Map<String, Object> row = new LinkedHashMap<>();
        for (String col : columnOrder) {
            row.put(col, columns.get(col).get(index));
        }
        return row;
    }

    /**
     * Conversion optimisée du DataFrame en CSV.
     */
    public String toCSV() {
        int nbRows = rowCount.get();
        StringBuilder sb = new StringBuilder(nbRows * (columnOrder.size() * 10));
        sb.append(String.join(",", columnOrder)).append("\n");
        for (int i = 0; i < nbRows; i++) {
            boolean first = true;
            for (String col : columnOrder) {
                if (!first) {
                    sb.append(",");
                }
                Object val = columns.get(col).get(i);
                if (val != null) {
                    sb.append(val.toString());
                }
                first = false;
            }
            sb.append("\n");
        }
        return sb.toString();
    }

    public int countRows() {
        return rowCount.get();
    }

    public void printAsCSV() {
        System.out.print(toCSV());
    }

    /**
     * Méthode de filtrage qui crée un nouveau DataFrame ne contenant que les lignes
     * pour lesquelles la valeur dans la colonne donnée correspond à celle recherchée.
     */
    public DataFrame filter(String columnName, Object value) {
        DataFrame filtered = new DataFrame();
        filtered.setTableName(this.tableName);
        if (!columns.containsKey(columnName)) {
            throw new IllegalArgumentException("La colonne '" + columnName + "' n'existe pas.");
        }
        filtered.initializeColumns(this.columnOrder, this.countRows());
        int filteredIndex = 0;
        for (int i = 0; i < countRows(); i++) {
            Object cellValue = columns.get(columnName).get(i);
            boolean match;
            if (cellValue instanceof Number && value instanceof String) {
                try {
                    long queryValue = Long.parseLong((String) value);
                    match = (((Number) cellValue).longValue() == queryValue);
                } catch (NumberFormatException e) {
                    match = false;
                }
            } else {
                match = String.valueOf(cellValue).equals(String.valueOf(value));
            }
            if (match) {
                filtered.setRow(filteredIndex, toRowArray(i));
                filteredIndex++;
            }
        }
        return filtered;
    }

    // Aide à convertir une ligne en tableau d'objets.
    private Object[] toRowArray(int index) {
        Object[] row = new Object[columnOrder.size()];
        for (int i = 0; i < columnOrder.size(); i++) {
            String col = columnOrder.get(i);
            row[i] = columns.get(col).get(index);
        }
        return row;
    }
}
