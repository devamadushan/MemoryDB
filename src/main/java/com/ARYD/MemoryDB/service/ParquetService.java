package com.ARYD.MemoryDB.service;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import java.io.IOException;


public class ParquetService {

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


    public void saveParquetFile(String filePath) {
        Path path = new Path(filePath);
        Configuration configuration = new Configuration();



    }
}
