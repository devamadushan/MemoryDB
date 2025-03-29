package com.ARYD.MemoryDB.controller;

import com.ARYD.MemoryDB.model.ParquetDataLoader;
import org.apache.avro.generic.GenericRecord;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;
import java.util.logging.Logger;

@RestController
@RequestMapping("/parquet")
public class ParquetController {
    private static final Logger LOGGER = Logger.getLogger(ParquetController.class.getName());
    private final ParquetDataLoader parquetDataLoader = new ParquetDataLoader();

    // Endpoint pour charger un fichier Parquet en mémoire
    @PostMapping("/load")
    public ResponseEntity<String> loadParquet() {
        String filePath = "src/data/test1002.parquet";
        int maxLines = 1000;
        LOGGER.info("Chargement du fichier : " + filePath);
        parquetDataLoader.loadParquetToMemory(filePath, maxLines);
        return ResponseEntity.ok("Fichier chargé en mémoire. Nombre total de lignes : " + parquetDataLoader.getDataMap().size());
    }

    // Endpoint pour récupérer un enregistrement par ID
    @GetMapping("/record/{id}")
    public ResponseEntity<GenericRecord> getRecord(@PathVariable int id) {
        GenericRecord record = parquetDataLoader.getRecordById(id);
        if (record == null) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(record);
    }

    // Endpoint pour récupérer tous les enregistrements chargés
    @GetMapping("/records")
    public ResponseEntity<Map<Integer, GenericRecord>> getAllRecords() {
        return ResponseEntity.ok(parquetDataLoader.getDataMap());
    }
}
