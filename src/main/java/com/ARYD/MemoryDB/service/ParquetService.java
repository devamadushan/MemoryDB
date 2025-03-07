package com.ARYD.MemoryDB.service;
import com.ARYD.MemoryDB.entity.Column;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
@RequiredArgsConstructor
@Slf4j
public class ParquetService {
    private final TableService tableService;

    public void readParquetFile(String filePath) {
        Path path = new Path(filePath);
        Configuration configuration = new Configuration();

        try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(
                HadoopInputFile.fromPath(path, configuration)).build()) {

            GenericRecord record;
            while ((record = reader.read()) != null) {
                System.out.println(record);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

           public void insertParquetIntoTable(String filePath, String tableName) {
            Path path = new Path(filePath);
            Configuration configuration = new Configuration();

            try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(
                    HadoopInputFile.fromPath(path, configuration)).build()) {

                GenericRecord record = reader.read(); // Lire la première ligne pour récupérer les colonnes
                if (record == null) {
                    System.out.println("Le fichier Parquet est vide.");
                    return;
                }

                // Vérifier si la table existe, sinon la créer avec les colonnes du Parquet
                if (!tableService.getTables().stream().anyMatch(t -> t.getName().equals(tableName))) {
                    List<Column> columns = new ArrayList<>();
                    for (org.apache.avro.Schema.Field field : record.getSchema().getFields()) {
                        columns.add(new Column(field.name(), field.schema().getType().getName())); // Détection automatique
                    }
                    tableService.createTable(tableName, columns);
                    System.out.println("Table créée automatiquement avec les colonnes : " + columns);
                }

                // Insérer les données
                do {
                    Map<String, Object> row = new HashMap<>();
                    for (org.apache.avro.Schema.Field field : record.getSchema().getFields()) {
                        row.put(field.name(), record.get(field.name()));
                    }
                    tableService.insertRow(tableName, row);
                } while ((record = reader.read()) != null);

                System.out.println("Données NYC Parquet insérées dans la table : " + tableName);

            } catch (IOException e) {
                System.err.println("Erreur lors de la lecture du fichier Parquet : " + e.getMessage());
            }
        }

    }

