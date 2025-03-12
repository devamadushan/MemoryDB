package com.ARYD.MemoryDB.controller;

import com.ARYD.MemoryDB.model.Table;
import com.ARYD.MemoryDB.model.ParquetLoader;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@RestController
@RequestMapping("/tables")
public class TableController {
    private final Map<String, Table> tables = new ConcurrentHashMap<>();

    @PostMapping("/{tableName}")
    public ResponseEntity<String> createTable(@PathVariable String tableName, @RequestBody Map<String, String> request) {
        String filePath = request.get("filePath");

        try {
            if (!tables.containsKey(tableName)) {
                tables.put(tableName, new Table(tableName, new ArrayList<>()));
            }

            Table table = tables.get(tableName);

            // Charger le fichier en batchs de 1000 lignes
            ParquetLoader.loadParquetFileParallel(filePath, table::addRow);

            return ResponseEntity.ok("Table " + tableName + " remplie avec succès.");
        } catch (IOException e) {
            return ResponseEntity.badRequest().body("Erreur de chargement du fichier : " + e.getMessage());
        }
    }

    @GetMapping("/{tableName}")
    public ResponseEntity<List<Map<String, Object>>> getAllData(@PathVariable String tableName) {
        Table table = tables.get(tableName);
        if (table == null) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(table.getAllRows());
    }

    @GetMapping("/test")
    public String test() {

        return "c'est bon ";
    }
}
