package com.ARYD.MemoryDB.controller;

import com.ARYD.MemoryDB.model.Table;
import com.ARYD.MemoryDB.model.ParquetLoader;
import com.ARYD.MemoryDB.repository.TableDao;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/tables")
public class TableController {
    private final TableDao tableDao; // Injection du DAO

    @Autowired
    public TableController(TableDao tableDao) {
        this.tableDao = tableDao;
    }

    @PostMapping("/{tableName}")
    public ResponseEntity<String> createTable(@PathVariable String tableName, @RequestBody Map<String, String> request) {
        String filePath = request.get("filePath");

        if (filePath == null || filePath.isEmpty()) {
            return ResponseEntity.badRequest().body("Le chemin du fichier est manquant.");
        }

        // Vérifier si le fichier existe avant de tenter de le charger
        if (!new java.io.File(filePath).exists()) {
            return ResponseEntity.badRequest().body("Fichier non trouvé : " + filePath);
        }

        try {
            if (!tableDao.tableExists(tableName)) {
                tableDao.saveTable(tableName, new Table(tableName , List.of()));
            }

            Table table = tableDao.getTable(tableName);

            ParquetLoader.loadParquetFileParallel(filePath, table::addRow);

            return ResponseEntity.ok("Table " + tableName + " remplie avec succès.");
        } catch (IOException e) {
            return ResponseEntity.badRequest().body("Erreur lors du chargement du fichier : " + e.getMessage());
        }
    }


    @GetMapping("/{tableName}")
    public ResponseEntity<List<Map<String, Object>>> getAllData(@PathVariable String tableName) {
        Table table = tableDao.getTable(tableName);
        if (table == null) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(table.getAllRows());
    }

    @GetMapping
    public ResponseEntity<Map<String, Table>> listTables() {
        return ResponseEntity.ok(tableDao.getAllTables());
    }

    @DeleteMapping("/{tableName}")
    public ResponseEntity<String> deleteTable(@PathVariable String tableName) {
        if (tableDao.tableExists(tableName)) {
            tableDao.deleteTable(tableName);
            return ResponseEntity.ok("Table " + tableName + " supprimée.");
        } else {
            return ResponseEntity.badRequest().body("Table " + tableName + " introuvable.");
        }
    }

    @GetMapping("/test")
    public String test() {
        return "c'est bon";
    }

    @GetMapping("/names")
    public ResponseEntity<String> getTablesNames() {
        if(!tableDao.getAllTables().isEmpty()) {
            return ResponseEntity.ok("Les Tables disponibles : " + String.join(", ", tableDao.getAllTables().keySet()));
        }
        else {
            return ResponseEntity.badRequest().body("Tables non disponble");
        }
    }


}
