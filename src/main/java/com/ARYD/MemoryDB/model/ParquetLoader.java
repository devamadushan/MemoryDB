package com.ARYD.MemoryDB.model;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;
import java.util.function.Consumer;

public class ParquetLoader {

    private static final int THREAD_COUNT = 4;  // Nb thread en parallèle
    private static final int BATCH_SIZE = 450;  // Nettoyage de mémoire toutes les 450 lignes
    private static final int MAX_LINES = 501; // Pour éviter les boucle infinie

    public static void loadParquetFileParallel(String filePath, Consumer<Map<String, Object>> rowConsumer) throws IOException {
        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
        Path path = new Path(filePath);
        Configuration conf = new Configuration();
        GroupReadSupport readSupport = new GroupReadSupport();
        ConcurrentLinkedQueue<Map<String, Object>> queue = new ConcurrentLinkedQueue<>();

        int rowId = 0;
        int batchCounter = 0;

        try (ParquetReader<Group> reader = ParquetReader.builder(readSupport, path).withConf(conf).build()) {
            Group group;

            while ((group = reader.read()) != null) {
                if (rowId >= MAX_LINES) {
                    System.out.println("Limite de lignes atteinte : " + MAX_LINES);
                    break;
                }

                Map<String, Object> record = new HashMap<>();
                record.put("_rowId", rowId++);

                MessageType schema = (MessageType) group.getType();
                for (int i = 0; i < schema.getFieldCount(); i++) {
                    String columnName = schema.getFieldName(i);
                    try {
                        record.put(columnName, group.getValueToString(i, 0));
                    } catch (Exception e) {
                        System.err.println("Erreur sur la ligne " + rowId + " : " + e.getMessage());
                        continue;
                    }
                }

                queue.offer(record);
                executor.submit(() -> rowConsumer.accept(queue.poll()));

                batchCounter++;

                if (batchCounter >= BATCH_SIZE) {
                    batchCounter = 0;
                    System.gc();
                    System.out.println("Nettoyage mémoire après " + BATCH_SIZE + " lignes...");
                }
            }
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

        System.out.println("Chargement du fichier terminé, total de lignes traitées : " + rowId);
    }


}
