package com.ARYD.MemoryDB.util;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.avro.generic.GenericRecord;
import org.springframework.stereotype.Component;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Classe utilitaire pour inspecter les fichiers Parquet
 */
@Component
@Slf4j
public class ParquetInspector {

    /**
     * Compte le nombre exact de lignes dans un fichier Parquet
     * Cette opération peut être coûteuse pour les grands fichiers car elle nécessite un scan complet
     * 
     * @param filePath Chemin du fichier Parquet
     * @param useFastCount Si true, utilise l'estimation à partir des métadonnées (plus rapide mais moins précis)
     * @return Nombre de lignes, ou -1 en cas d'erreur
     */
    public long countRowsInParquetFile(String filePath, boolean useFastCount) {
        if (useFastCount) {
            try {
                Path path = new Path(filePath);
                Configuration configuration = new Configuration();
                configuration.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
                configuration.set("fs.file.impl.disable.cache", "true");
                
                // Lire les métadonnées du fichier
                ParquetMetadata metadata = ParquetFileReader.readFooter(configuration, path);
                long estimatedRows = metadata.getBlocks().stream()
                        .mapToLong(block -> block.getRowCount())
                        .sum();
                
                log.info("Estimation rapide du nombre de lignes dans {}: {}", filePath, estimatedRows);
                return estimatedRows;
            } catch (Exception e) {
                log.error("Erreur lors de l'estimation du nombre de lignes", e);
                return -1;
            }
        } else {
            log.info("Comptage précis des lignes dans {}, cela peut prendre un moment...", filePath);
            try {
                Path path = new Path(filePath);
                Configuration configuration = new Configuration();
                configuration.set("hadoop.security.authentication", "simple");
                configuration.set("hadoop.security.authorization", "false");
                configuration.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
                configuration.set("fs.file.impl.disable.cache", "true");
                configuration.set("parquet.strings.signed-min-max.enabled", "false");
                
                // Créer un lecteur et compter les lignes une par une
                long startTime = System.currentTimeMillis();
                long rowCount = 0;
                try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(
                        HadoopInputFile.fromPath(path, configuration)).build()) {
                    
                    while (reader.read() != null) {
                        rowCount++;
                        
                        // Log de progression toutes les 500,000 lignes
                        if (rowCount % 500000 == 0) {
                            long elapsed = System.currentTimeMillis() - startTime;
                            double rowsPerSecond = (rowCount * 1000.0) / elapsed;
                            log.info("Comptage en cours: {} lignes ({} lignes/sec)", rowCount, (int)rowsPerSecond);
                        }
                    }
                }
                
                long elapsed = System.currentTimeMillis() - startTime;
                log.info("Comptage terminé: {} lignes en {} secondes", rowCount, elapsed / 1000.0);
                return rowCount;
            } catch (Exception e) {
                log.error("Erreur lors du comptage des lignes", e);
                return -1;
            }
        }
    }

    /**
     * Inspecte un fichier Parquet et retourne des informations sur sa structure
     */
    public Map<String, Object> inspectParquetFile(String filePath) {
        Map<String, Object> result = new HashMap<>();
        
        File file = new File(filePath);
        if (!file.exists() || !file.isFile()) {
            result.put("error", "Le fichier n'existe pas ou n'est pas un fichier valide");
            return result;
        }
        
        result.put("filePath", filePath);
        result.put("fileSize", file.length());
        
        try {
            Path path = new Path(filePath);
            Configuration configuration = new Configuration();
            configuration.set("hadoop.security.authentication", "simple");
            configuration.set("hadoop.security.authorization", "false");
            configuration.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
            configuration.set("fs.file.impl.disable.cache", "true");
            
            // Lire les métadonnées du fichier Parquet
            ParquetMetadata metadata = ParquetFileReader.readFooter(configuration, path);
            result.put("rowGroups", metadata.getBlocks().size());
            result.put("schema", metadata.getFileMetaData().getSchema().toString());
            result.put("createdBy", metadata.getFileMetaData().getCreatedBy());
            
            // Estimation du nombre de lignes à partir des métadonnées
            long estimatedRows = metadata.getBlocks().stream()
                    .mapToLong(block -> block.getRowCount())
                    .sum();
            result.put("estimatedRows", estimatedRows);
            
            // Essayer de lire un exemple d'enregistrement
            try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(
                    HadoopInputFile.fromPath(path, configuration)).build()) {
                
                GenericRecord record = reader.read();
                if (record != null) {
                    Map<String, Object> sampleRecord = new HashMap<>();
                    Schema schema = record.getSchema();
                    result.put("avroSchema", schema.toString(true));
                    
                    Map<String, String> columns = new HashMap<>();
                    for (Schema.Field field : schema.getFields()) {
                        String name = field.name();
                        String type = field.schema().getType().getName();
                        columns.put(name, type);
                        
                        Object value = record.get(name);
                        if (value != null) {
                            sampleRecord.put(name, value.toString());
                        } else {
                            sampleRecord.put(name, null);
                        }
                    }
                    
                    result.put("columns", columns);
                    result.put("sampleRecord", sampleRecord);
                    
                    // Ajouter des informations sur le comptage des lignes
                    result.put("note", "Pour obtenir le nombre exact de lignes, utilisez l'API /count-rows. " +
                                       "Le nombre estimé est basé sur les métadonnées du fichier.");
                } else {
                    result.put("warning", "Le fichier est vide, aucun enregistrement trouvé");
                }
            }
            
            return result;
        } catch (Exception e) {
            log.error("Erreur lors de l'inspection du fichier Parquet", e);
            result.put("error", "Erreur lors de l'inspection du fichier: " + e.getMessage());
            return result;
        }
    }
    
    /**
     * Valide si un fichier est un fichier Parquet valide
     */
    public boolean isValidParquetFile(String filePath) {
        File file = new File(filePath);
        if (!file.exists() || !file.isFile()) {
            log.error("Le fichier n'existe pas ou n'est pas un fichier: {}", filePath);
            return false;
        }
        
        log.info("Validation du fichier Parquet: {}, taille: {} bytes", filePath, file.length());
        
        try {
            Path path = new Path(filePath);
            Configuration configuration = new Configuration();
            configuration.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
            configuration.set("fs.file.impl.disable.cache", "true");
            configuration.set("hadoop.security.authentication", "simple");
            configuration.set("hadoop.security.authorization", "false");
            configuration.set("parquet.strings.signed-min-max.enabled", "false");
            
            log.debug("Tentative de lecture des métadonnées du fichier Parquet...");
            
            // Tenter de lire les métadonnées
            ParquetMetadata metadata = ParquetFileReader.readFooter(configuration, path);
            log.info("Lecture des métadonnées réussie. Nombre de blocs: {}, schéma: {}", 
                     metadata.getBlocks().size(), 
                     metadata.getFileMetaData().getSchema().toString());
            
            // Essayer de lire au moins un enregistrement pour confirmer que le fichier est bien valide
            try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(
                    HadoopInputFile.fromPath(path, configuration)).build()) {
                
                log.debug("Tentative de lecture d'un enregistrement...");
                GenericRecord record = reader.read();
                
                if (record != null) {
                    log.info("Lecture d'un enregistrement réussie. Fichier Parquet valide.");
                    return true;
                } else {
                    log.warn("Aucun enregistrement dans le fichier. Fichier Parquet vide.");
                    // On considère quand même le fichier comme valide, même s'il est vide
                    return true;
                }
            } catch (Exception e) {
                log.error("Erreur lors de la lecture d'un enregistrement: {}", e.getMessage(), e);
                return false;
            }
        } catch (Exception e) {
            log.error("Le fichier n'est pas un fichier Parquet valide: {}. Erreur: {}", filePath, e.getMessage(), e);
            return false;
        }
    }
} 
 