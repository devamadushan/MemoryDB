package com.ARYD.MemoryDB.service;

import com.ARYD.MemoryDB.entity.DataFrame;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.ColumnIOFactory;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
@Slf4j
public class ParquetService {

    private static final int MAX_ROWS = 1_000_000; // Limite de lecture
    private static final int THREAD_COUNT = Runtime.getRuntime().availableProcessors() * 2; // Doubler le nombre de threads

    public void readParquetFile(String filePath, DataFrame df) {
        Path path = new Path(filePath);
        Configuration configuration = new Configuration();

        // Optimisations Hadoop/Parquet
        configuration.set("parquet.page.size", "1048576"); // 1MB page size
        configuration.set("parquet.read.ahead", "true"); // Activer la lecture anticipée
        configuration.set("parquet.memory.pool.ratio", "0.9"); // 90% de la mémoire disponible
        configuration.set("parquet.block.size", "268435456"); // 256MB block size
        configuration.set("parquet.page.verify-checksum", "false"); // Désactiver la vérification des checksums
        configuration.set("parquet.filter.dictionary.enabled", "true"); // Activer le filtrage par dictionnaire
        configuration.set("parquet.filter.record-level.enabled", "true"); // Activer le filtrage au niveau des enregistrements
        configuration.set("parquet.enable.dictionary", "true"); // Activer le dictionnaire

        // Utiliser un ThreadPoolExecutor avec une queue liée pour un meilleur contrôle
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                THREAD_COUNT,
                THREAD_COUNT,
                60L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(THREAD_COUNT * 2),
                new ThreadPoolExecutor.CallerRunsPolicy()); // Politique qui exécute la tâche dans le thread appelant si la queue est pleine

        try {
            // Première passe : lecture des métadonnées et détermination des colonnes
            Set<String> columnNames = readSchemaAndColumns(path, configuration);

            // Initialisation des colonnes dans le DataFrame
            for (String columnName : columnNames) {
                df.addColumn(columnName);
            }

            // Deuxième passe : lecture des données en parallèle par colonnes
            List<Future<?>> futures = new ArrayList<>();

            // Diviser les colonnes en groupes pour le traitement parallèle
            List<List<String>> columnGroups = splitIntoGroups(new ArrayList<>(columnNames), THREAD_COUNT);

            for (List<String> columnGroup : columnGroups) {
                futures.add(executor.submit(() -> {
                    readColumnsData(path, configuration, df, columnGroup, MAX_ROWS);
                }));
            }

            // Attendre la fin de toutes les tâches
            for (Future<?> future : futures) {
                try {
                    future.get();
                } catch (InterruptedException | ExecutionException e) {
                    log.error("Erreur lors de l'exécution d'une tâche de lecture", e);
                }
            }

            log.info("Lecture terminée : {} colonnes chargées avec succès.", columnNames.size());

        } catch (Exception e) {
            log.error("Erreur lors de la lecture du fichier Parquet : ", e);
        } finally {
            executor.shutdown();
            try {
                if (!executor.awaitTermination(5, TimeUnit.MINUTES)) {
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                executor.shutdownNow();
            }
        }
    }

    private Set<String> readSchemaAndColumns(Path path, Configuration configuration) throws IOException {
        Set<String> columnNames = new HashSet<>();

        try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(
                        HadoopInputFile.fromPath(path, configuration))
                .build()) {

            // Lire le premier enregistrement pour obtenir le schéma
            GenericRecord record = reader.read();
            if (record != null) {
                for (Schema.Field field : record.getSchema().getFields()) {
                    columnNames.add(field.name());
                }
            }
        }

        return columnNames;
    }

    private void readColumnsData(Path path, Configuration configuration, DataFrame df, List<String> columnNames, int maxRows) {
        try {
            // Créer un tableau pour stocker les valeurs de colonnes
            Map<String, List<Object>> columnValues = new HashMap<>();
            for (String columnName : columnNames) {
                // Pré-allocation avec la taille maximale pour éviter les redimensionnements
                columnValues.put(columnName, new ArrayList<>(maxRows));
            }

            // Lecteur optimisé pour les colonnes spécifiées
            try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(
                            HadoopInputFile.fromPath(path, configuration))
                    .useRecordFilter() // Pas de filtrage
                    .build()) {

                GenericRecord record;
                int rowCount = 0;

                while ((record = reader.read()) != null && rowCount < maxRows) {
                    for (String columnName : columnNames) {
                        Object value = record.get(columnName);
                        columnValues.get(columnName).add(value);
                    }
                    rowCount++;

                    // Log périodique pour suivre la progression
                    if (rowCount % 250000 == 0) {
                        log.debug("Lecture du groupe de colonnes {} : {} lignes traitées",
                                columnNames.get(0), rowCount);
                    }
                }
            }

            // Mettre à jour le DataFrame en une seule opération par colonne
            synchronized (df) {
                for (String columnName : columnNames) {
                    List<Object> values = columnValues.get(columnName);
                    // Optimisation : utiliser directement la liste interne du DataFrame
                    df.getColumns().get(columnName).get("values").addAll(values);
                }
            }

        } catch (IOException e) {
            log.error("Erreur lors de la lecture des colonnes {}", columnNames, e);
        }
    }

    private <T> List<List<T>> splitIntoGroups(List<T> items, int groupCount) {
        List<List<T>> groups = new ArrayList<>(groupCount);
        for (int i = 0; i < groupCount; i++) {
            groups.add(new ArrayList<>());
        }

        for (int i = 0; i < items.size(); i++) {
            groups.get(i % groupCount).add(items.get(i));
        }

        return groups;
    }
    // Méthode alternative utilisant une approche hybride pour un maximum de performances
    public void readParquetFileHybrid(String filePath, DataFrame df) {
        Path path = new Path(filePath);
        Configuration configuration = new Configuration();

        // Optimisations Hadoop/Parquet
        configuration.set("parquet.page.size", "1048576"); // 1MB page size
        configuration.set("parquet.read.ahead", "true");
        configuration.set("parquet.memory.pool.ratio", "0.9");
        configuration.set("fs.file.buffer.size", "262144"); // 256KB
        configuration.set("io.file.buffer.size", "262144"); // 256KB

        // Désactiver la compression pour la lecture
        configuration.set("parquet.compression", "UNCOMPRESSED");

        // Utiliser des buffers en mémoire directs
        System.setProperty("parquet.enable.unsafe", "true");

        // Préparer un pool d'exécution pour la parallélisation
        ForkJoinPool customPool = new ForkJoinPool(THREAD_COUNT);

        try {
            // Lire les métadonnées du fichier Parquet
            org.apache.parquet.hadoop.ParquetFileReader parquetFileReader =
                    org.apache.parquet.hadoop.ParquetFileReader.open(HadoopInputFile.fromPath(path, configuration));
            org.apache.parquet.hadoop.metadata.FileMetaData fileMetaData = parquetFileReader.getFooter().getFileMetaData();
            org.apache.parquet.schema.MessageType schema = fileMetaData.getSchema();

            // Obtenir la liste des colonnes
            List<String> columnNames = new ArrayList<>();
            for (org.apache.parquet.schema.Type field : schema.getFields()) {
                columnNames.add(field.getName());
            }

            // Initialiser le DataFrame avec toutes les colonnes
            for (String columnName : columnNames) {
                df.addColumn(columnName);
            }

            // Estimer le nombre total de lignes
            int estimatedRows = Math.min(MAX_ROWS,
                    (int)Arrays.stream(parquetFileReader.getRowGroups().stream()
                            .mapToLong(rg -> rg.getRowCount())
                            .toArray()).sum());

            // Créer des listes pré-dimensionnées pour chaque colonne
            Map<String, List<Object>> columnValuesMap = new HashMap<>();
            for (String column : columnNames) {
                columnValuesMap.put(column, new ArrayList<>(estimatedRows));
            }

            // Fermer le lecteur de métadonnées
            parquetFileReader.close();

            // Créer un lecteur Parquet optimisé pour la lecture par colonnes
            ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(
                            HadoopInputFile.fromPath(path, configuration))
                    .build();

            // File d'attente pour traitement parallèle par lots
            BlockingQueue<List<GenericRecord>> recordBatches = new LinkedBlockingQueue<>(THREAD_COUNT * 2);
            AtomicBoolean readerComplete = new AtomicBoolean(false);

            // Tâche de lecture depuis le fichier Parquet
            CompletableFuture<Void> readerTask = CompletableFuture.runAsync(() -> {
                try {
                    List<GenericRecord> batch = new ArrayList<>(10000);
                    GenericRecord record;
                    int rowCount = 0;

                    while ((record = reader.read()) != null && rowCount < MAX_ROWS) {
                        batch.add(record);
                        rowCount++;

                        if (batch.size() >= 10000) {
                            recordBatches.put(new ArrayList<>(batch));
                            batch.clear();
                        }
                    }

                    // Traiter le dernier lot s'il existe
                    if (!batch.isEmpty()) {
                        recordBatches.put(new ArrayList<>(batch));
                    }

                    log.info("Lecture terminée: {} lignes lues", rowCount);
                } catch (Exception e) {
                    log.error("Erreur lors de la lecture du fichier Parquet", e);
                } finally {
                    readerComplete.set(true);
                    try {
                        reader.close();
                    } catch (IOException e) {
                        log.error("Erreur lors de la fermeture du lecteur", e);
                    }
                }
            });

            // Créer des tâches de traitement pour chaque lot
            List<CompletableFuture<Void>> processingTasks = new ArrayList<>();
            for (int i = 0; i < THREAD_COUNT; i++) {
                processingTasks.add(CompletableFuture.runAsync(() -> {
                    try {
                        while (!readerComplete.get() || !recordBatches.isEmpty()) {
                            List<GenericRecord> batch = recordBatches.poll(100, TimeUnit.MILLISECONDS);
                            if (batch != null) {
                                processBatch(batch, columnNames, columnValuesMap);
                            }
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        log.error("Traitement interrompu", e);
                    }
                }, customPool));
            }

            // Attendre que toutes les tâches soient terminées
            CompletableFuture.allOf(
                    CompletableFuture.allOf(processingTasks.toArray(new CompletableFuture[0])),
                    readerTask
            ).join();

            // Mettre à jour le DataFrame avec toutes les valeurs
            synchronized (df) {
                for (String column : columnNames) {
                    List<Object> values = columnValuesMap.get(column);
                    df.getColumns().get(column).get("values").addAll(values);
                }
                // Mettre à jour le compteur de lignes
                ((AtomicInteger)df.getClass().getDeclaredField("rowCount").get(df))
                        .set(columnValuesMap.get(columnNames.get(0)).size());
            }

            log.info("Chargement terminé : {} lignes dans le DataFrame", df.countRows());

        } catch (Exception e) {
            log.error("Erreur lors de la lecture du fichier Parquet", e);
        } finally {
            customPool.shutdown();
        }
    }

    // Méthode pour traiter un lot d'enregistrements
    private void processBatch(List<GenericRecord> records, List<String> columnNames,
                              Map<String, List<Object>> columnValuesMap) {
        // Pour chaque colonne, extraire les valeurs de tous les enregistrements
        for (String column : columnNames) {
            List<Object> columnValues = columnValuesMap.get(column);
            synchronized (columnValues) {
                for (GenericRecord record : records) {
                    columnValues.add(record.get(column));
                }
            }
        }
    }
}