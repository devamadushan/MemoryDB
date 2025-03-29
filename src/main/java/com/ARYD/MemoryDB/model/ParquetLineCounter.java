package com.ARYD.MemoryDB.model;

import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.hadoop.fs.Path;

public class ParquetLineCounter {
    public static void main(String[] args) {
        String parquetFilePath = "src/data/test1002" +
                ".parquet";  // Mets le chemin de ton fichier

        long startTime = System.currentTimeMillis();
        int totalLines = countParquetLines(parquetFilePath);
        long elapsedTime = System.currentTimeMillis() - startTime;

        System.out.println("Total lignes trouvées : " + totalLines);
        System.out.println("Temps d'exécution : " + (elapsedTime / 1000.0) + " secondes");
    }

    public static int countParquetLines(String filePath) {
        int count = 0;
        try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(new Path(filePath)).build()) {
            while (reader.read() != null) {
                count++;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return count;
    }
}
