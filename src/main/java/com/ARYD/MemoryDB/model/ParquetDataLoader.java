package com.ARYD.MemoryDB.model;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.hadoop.fs.Path;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ParquetDataLoader {
    private static final Logger LOGGER = Logger.getLogger(ParquetDataLoader.class.getName());
    private final Map<Integer, GenericRecord> dataMap;
    private int currentMaxId;
    private Schema recordSchema;

    public ParquetDataLoader() {
        this.dataMap = new ConcurrentHashMap<>();
        this.currentMaxId = 0;
    }

    public void loadParquetToMemory(String parquetFilePath, int maxLines) {
        long startTime = System.currentTimeMillis();
        try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(new Path(parquetFilePath)).build()) {
            List<GenericRecord> records = new ArrayList<>();
            GenericRecord record;
            while ((record = reader.read()) != null && records.size() < maxLines) {
                records.add(record);
            }
            if (!records.isEmpty() && recordSchema == null) {
                recordSchema = records.get(0).getSchema();
            }
            Map<Integer, GenericRecord> loadedRecords = records.stream()
                    .collect(Collectors.toMap(r -> ++currentMaxId, r -> r));
            dataMap.putAll(loadedRecords);
            LOGGER.info("Fichier chargé : " + loadedRecords.size() + " lignes");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Erreur lors du chargement : " + e.getMessage(), e);
        }
        long elapsedTime = System.currentTimeMillis() - startTime;
        LOGGER.info("Temps total : " + (elapsedTime / 1000.0) + " secondes");
    }

    public void add(String parquetFilePath, int maxLines) {
        LOGGER.info("Ajout de nouvelles lignes depuis : " + parquetFilePath);
        loadParquetToMemory(parquetFilePath, maxLines);
    }

    public GenericRecord getRecordById(int id) {
        return dataMap.get(id);
    }

    public void printAllRecords() {
        dataMap.forEach((id, record) -> System.out.println("ID " + id + ": " + record));
    }

    public Map<Integer, GenericRecord> getDataMap() {
        return Collections.unmodifiableMap(dataMap);
    }

    public static void main(String[] args) {
        ParquetDataLoader loader = new ParquetDataLoader();
        //long startTime = System.currentTimeMillis();
        loader.loadParquetToMemory("src/data/test1002.parquet", 6_500_000);
        //loader.add("src/data/test1002.parquet", 6_000_000);
        LOGGER.info("Total de lignes après chargement : " + loader.getDataMap().size());
        //long elapsedTime = System.currentTimeMillis() - startTime;
       // LOGGER.info("Temps total : " + (elapsedTime / 1000.0) + " secondes");
    }
}
