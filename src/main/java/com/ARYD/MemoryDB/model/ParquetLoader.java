package com.ARYD.MemoryDB.model;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

public class ParquetLoader {

    private static final int BATCH_SIZE = 1000; // Lire en batchs de 1000 lignes

    public static void loadParquetFile(String filePath, Consumer<Map<String, Object>> rowConsumer) throws IOException {
        Path path = new Path(filePath);
        Configuration conf = new Configuration();
        GroupReadSupport readSupport = new GroupReadSupport();

        try (ParquetReader<Group> reader = ParquetReader.builder(readSupport, path).withConf(conf).build()) {
            Group group;
            int rowId = 0;
            int batchCounter = 0;

            while ((group = reader.read()) != null) {
                Map<String, Object> record = new HashMap<>();
                record.put("_rowId", rowId++);

                MessageType schema = (MessageType) group.getType();
                for (int i = 0; i < schema.getFieldCount(); i++) {
                    String columnName = schema.getFieldName(i);
                    record.put(columnName, group.getValueToString(i, 0));
                }

                rowConsumer.accept(record); // Ajouter la ligne directement à la table
                batchCounter++;

                // Tous les 1000 lignes, on force Java à libérer la mémoire
                if (batchCounter >= BATCH_SIZE) {
                    batchCounter = 0;
                    System.gc(); // Libérer la mémoire
                }
            }
        }
    }
}
