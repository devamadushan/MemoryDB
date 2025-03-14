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
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
@Slf4j
public class ParquetService {

    private static final int BATCH_SIZE = 100_000;  // Taille du buffer augmentée à 100k
    private static final int MAX_ROWS = 1_000_000; // Limite de lecture
    private static final int THREAD_COUNT = Runtime.getRuntime().availableProcessors(); // Nombre de threads

    public void readParquetFile(String filePath, DataFrame df) {
        Path path = new Path(filePath);
        Configuration configuration = new Configuration();
        configuration.set("parquet.read.ahead", "false"); // Désactive la lecture anticipée
        configuration.set("parquet.page.verify-checksum", "false"); // Désactive la vérification des checksums
        configuration.set("parquet.memory.pool.ratio", "0.7"); // Utilise 70% de la mémoire dispo

        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);

        try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(
                HadoopInputFile.fromPath(path, configuration)).build()) {

            GenericRecord record;
            boolean firstRow = true;
            int rowCount = 0;
            List<Map<String, Object>> buffer = new ArrayList<>(); // Buffer temporaire

            while ((record = reader.read()) != null && rowCount < MAX_ROWS) {
                Map<String, Object> rowData = new LinkedHashMap<>();

                for (Schema.Field field : record.getSchema().getFields()) {
                    String columnName = field.name();
                    Object value = record.get(columnName);

                    if (firstRow) {
                        df.addColumn(columnName); // Ajouter la colonne si elle n'existe pas encore
                    }

                    rowData.put(columnName, value);
                }

                buffer.add(rowData);
                rowCount++;

                if (buffer.size() >= BATCH_SIZE) {
                    // Copie locale du buffer pour éviter qu'il ne soit modifié par un autre thread
                    List<Map<String, Object>> batchToProcess = new ArrayList<>(buffer);
                    buffer.clear();

                    // Traite le batch en parallèle
                    executor.submit(() -> flushBufferToDataFrame(batchToProcess, df));
                    log.info("Chargé {} lignes...", rowCount);
                }

                firstRow = false;
            }

            if (!buffer.isEmpty()) {
                executor.submit(() -> flushBufferToDataFrame(buffer, df));
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

    // Ajoute de manière thread-safe les lignes du buffer dans le DataFrame
    private void flushBufferToDataFrame(List<Map<String, Object>> buffer, DataFrame df) {
        synchronized (df) { // Bloc synchronized sur le DataFrame pour éviter les conflits
            for (Map<String, Object> row : buffer) {
                df.addRow(row);
            }
        }
    }
}