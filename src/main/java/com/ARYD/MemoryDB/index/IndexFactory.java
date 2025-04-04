package com.ARYD.MemoryDB.index;

import com.ARYD.MemoryDB.storage.Table;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class IndexFactory {
    
    public Index createIndex(Table table, List<String> columns) {
        // Pour l'instant, on crée un HashIndex par défaut
        // On pourra ajouter plus tard une logique pour choisir le type d'index
        // en fonction des caractéristiques des colonnes
        return new HashIndex(table, columns.toArray(new String[0]));
    }

    public Index createHashIndex(Table table, List<String> columns) {
        return new HashIndex(table, columns.toArray(new String[0]));
    }

    // Désactivée pour l'instant car TreeIndex n'est pas encore implémenté
    /*
    public Index createTreeIndex(Table table, List<String> columns) {
        return new TreeIndex(table, columns.toArray(new String[0]));
    }
    */
} 
 