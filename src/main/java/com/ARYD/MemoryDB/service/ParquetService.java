package com.ARYD.MemoryDB.service;
import com.ARYD.MemoryDB.entity.DataFrame;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

@Service
@RequiredArgsConstructor
@Slf4j
public class ParquetService {

    public void readParquetFile(String filePath, DataFrame df) {
        Path path = new Path(filePath);
        Configuration configuration = new Configuration();

        try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(
                HadoopInputFile.fromPath(path, configuration)).build()) {

            GenericRecord record;
            boolean firstRow = true;
            int rowCount = 0;
            int maxRows = 10000000; // On ne veut lire que 10 lignes

            while ((record = reader.read()) != null && rowCount < maxRows) {
                Map<String, Object> rowData = new LinkedHashMap<>();

                for (Schema.Field field : record.getSchema().getFields()) {
                    String columnName = field.name();
                    Object value = record.get(columnName);

                    if (firstRow) {
                        df.addColumn(columnName); // Ajouter la colonne si elle n'existe pas encore
                    }

                    rowData.put(columnName, value);
                }

                df.addRow(rowData);
                firstRow = false;
                rowCount++;
            }

            log.info("Lecture du fichier Parquet terminée avec {} lignes (limite: {}).", rowCount, maxRows);

        } catch (IOException e) {
            log.error("Erreur lors de la lecture du fichier Parquet : ", e);
        }
    }
}