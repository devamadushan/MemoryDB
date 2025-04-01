package com.ARYD.MemoryDB.service;

import com.ARYD.MemoryDB.entity.DataFrame;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

@Service
@RequiredArgsConstructor
@Slf4j
public class ParquetService {

    // Taille du batch ajustée
    private static final int BATCH_SIZE = 50_000;
    private static final int MAX_ROWS = 10_000_000; // Limite de lecture
    private static final int THREAD_COUNT = Runtime.getRuntime().availableProcessors();

    public void readParquetFile(String filePath, DataFrame df) {
        Path path = new Path(filePath);
        Configuration configuration = new Configuration();
        configuration.set("parquet.read.ahead", "false");
        configuration.set("parquet.page.verify-checksum", "false");
        configuration.set("parquet.memory.pool.ratio", "0.7");

        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
        // Compteur global pour assigner l'index des lignes sans conflit
        AtomicInteger globalRowIndex = new AtomicInteger(0);

        try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(
                HadoopInputFile.fromPath(path, configuration)).build()) {

            GenericRecord record;
            // Buffer de lignes, chaque ligne est représentée par un Object[]
            List<Object[]> buffer = new ArrayList<>(BATCH_SIZE);
            boolean initialized = false;
            int rowCount = 0;
            List<String> columnOrder = new ArrayList<>();

            while ((record = reader.read()) != null && rowCount < MAX_ROWS) {
                // À la première ligne, définir l'ordre des colonnes et initialiser la DataFrame
                if (!initialized) {
                    Schema schema = record.getSchema();
                    for (Schema.Field field : schema.getFields()) {
                        columnOrder.add(field.name());
                    }
                    df.initializeColumns(columnOrder, MAX_ROWS);
                    initialized = true;
                }
                // Créer un tableau pour la ligne
                int numColumns = columnOrder.size();
                Object[] rowData = new Object[numColumns];
                for (int i = 0; i < numColumns; i++) {
                    String col = columnOrder.get(i);
                    rowData[i] = record.get(col);
                }
                buffer.add(rowData);
                rowCount++;

                if (buffer.size() >= BATCH_SIZE) {
                    int batchSize = buffer.size();
                    int startIndex = globalRowIndex.getAndAdd(batchSize);
                    // Copie locale pour le traitement parallèle
                    List<Object[]> batchToFlush = new ArrayList<>(buffer);
                    buffer.clear();

                    // Traitement du batch en parallèle
                    executor.submit(() -> flushBufferToDataFrame(batchToFlush, startIndex, df));
                    log.info("Chargé {} lignes...", rowCount);
                }
            }

            // Traitement du dernier batch s'il en reste
            if (!buffer.isEmpty()) {
                int batchSize = buffer.size();
                int startIndex = globalRowIndex.getAndAdd(batchSize);
                executor.submit(() -> flushBufferToDataFrame(buffer, startIndex, df));
                log.info("Finalisation - Chargé {} lignes.", rowCount);
            }

            log.info("Lecture terminée : {} lignes chargées (limite: {}).", rowCount, MAX_ROWS);

        } catch (IOException e) {
            log.error("Erreur lors de la lecture du fichier Parquet : ", e);
        } finally {
            executor.shutdown();
            try {
                if (!executor.awaitTermination(10, TimeUnit.MINUTES)) {
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                executor.shutdownNow();
            }
        }
    }

    /**
     * Pour chaque ligne du buffer, affecte la ligne dans la DataFrame à l'index calculé.
     * Chaque thread écrit dans une zone exclusive (déterminée par startIndex) sans contention.
     */
    private void flushBufferToDataFrame(List<Object[]> buffer, int startIndex, DataFrame df) {
        for (int i = 0; i < buffer.size(); i++) {
            df.setRow(startIndex + i, buffer.get(i));
        }
    }
}
