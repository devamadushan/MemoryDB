package com.ARYD.MemoryDB.index;

import com.ARYD.MemoryDB.storage.Table;
import com.ARYD.MemoryDB.query.Operator;
import com.ARYD.MemoryDB.query.Predicate;
import org.apache.commons.lang3.ArrayUtils;

import java.io.IOException;
import java.util.Collection;

public interface Index {
    /**
     * Retourne la table associée à cet index
     */
    Table getTable();

    /**
     * Retourne les colonnes indexées
     */
    String[] getIndexedColumns();

    /**
     * Vérifie si l'index peut être utilisé avec un prédicat donné
     */
    default boolean canBeUsedWithPredicate(Predicate predicate) {
        boolean containsColumn = ArrayUtils.contains(getIndexedColumns(), predicate.getColumn());
        boolean isOperatorCompatible = isOperatorCompatible(predicate.getOperator());
        return containsColumn && isOperatorCompatible;
    }

    /**
     * Vérifie si un opérateur est compatible avec cet index
     */
    boolean isOperatorCompatible(Operator op);

    /**
     * Retourne la liste des opérateurs compatibles avec cet index
     */
    Operator[] compatibleOperators();

    /**
     * Retourne les IDs des lignes correspondant au prédicat
     */
    int[] getIdsFromPredicate(Predicate predicate) throws IndexException;

    /**
     * Rafraîchit l'index avec les données des colonnes
     */
    void refreshIndexWithColumnsData(boolean verbose);

    /**
     * Ajoute une valeur à l'index
     */
    void addValue(Object value, int position) throws IOException;

    /**
     * Persiste l'index sur le disque
     */
    void flushOnDisk() throws IOException;
} 
 