package com.ARYD.utils;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

import javax.inject.Singleton;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Utilitaires pour travailler avec des fichiers Parquet
 */
@Singleton
public class ParquetUtils {
    
    private static final Logger logger = Logger.getLogger(ParquetUtils.class.getName());
    
    public ParquetUtils() {
        // Configuration Hadoop pour Java 17+
        System.setProperty("hadoop.home.dir", "/");
        System.setProperty("java.security.manager", "allow");
        System.setProperty("HADOOP_USER_NAME", "hadoop");
    }
    
    /**
     * Vérifie si un fichier est un fichier Parquet valide
     */
    public boolean isValidParquetFile(String filePath) {
        if (filePath == null || filePath.isEmpty()) {
            return false;
        }
        
        File file = new File(filePath);
        if (!file.exists() || !file.isFile()) {
            logger.warning("Le fichier n'existe pas ou n'est pas un fichier: " + filePath);
            return false;
        }
        
        try {
            Configuration conf = createHadoopConfiguration();
            Path path = new Path(filePath);
            
            // Tenter de lire les métadonnées Parquet
            ParquetMetadata metadata = ParquetFileReader.readFooter(conf, path);
            return metadata != null && metadata.getFileMetaData() != null;
        } catch (Exception e) {
            logger.warning("Erreur lors de la validation du fichier Parquet: " + e.getMessage());
            return false;
        }
    }
    
    /**
     * Inspecte un fichier Parquet et retourne des informations sur sa structure
     */
    public Map<String, Object> inspectParquetFile(String filePath) {
        Map<String, Object> result = new HashMap<>();
        
        if (!isValidParquetFile(filePath)) {
            result.put("error", "Le fichier n'est pas un fichier Parquet valide");
            result.put("filePath", filePath);
            return result;
        }
        
        try {
            Configuration conf = createHadoopConfiguration();
            Path path = new Path(filePath);
            
            // Lire les métadonnées
            ParquetMetadata metadata = ParquetFileReader.readFooter(conf, path);
            MessageType schema = metadata.getFileMetaData().getSchema();
            
            // Informations générales
            result.put("filePath", filePath);
            result.put("fileSize", new File(filePath).length());
            result.put("createdBy", metadata.getFileMetaData().getCreatedBy());
            result.put("rowGroups", metadata.getBlocks().size());
            
            // Estimation du nombre total de lignes
            long estimatedRows = metadata.getBlocks().stream()
                    .mapToLong(block -> block.getRowCount())
                    .sum();
            result.put("estimatedRows", estimatedRows);
            
            // Informations sur le schéma
            List<Map<String, Object>> columns = new ArrayList<>();
            for (Type field : schema.getFields()) {
                Map<String, Object> columnInfo = new HashMap<>();
                columnInfo.put("name", field.getName());
                columnInfo.put("type", convertParquetTypeToString(field));
                columnInfo.put("repetition", field.getRepetition().name());
                columns.add(columnInfo);
            }
            result.put("columns", columns);
            
            // Lire une ligne d'exemple
            try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(
                    HadoopInputFile.fromPath(path, conf))
                    .withConf(conf)
                    .build()) {
                
                GenericRecord record = reader.read();
                if (record != null) {
                    Map<String, Object> sampleRow = new HashMap<>();
                    for (Schema.Field field : record.getSchema().getFields()) {
                        String name = field.name();
                        Object value = record.get(name);
                        sampleRow.put(name, value != null ? value.toString() : null);
                    }
                    result.put("sampleRow", sampleRow);
                }
            }
            
            return result;
        } catch (Exception e) {
            logger.warning("Erreur lors de l'inspection du fichier Parquet: " + e.getMessage());
            result.put("error", "Erreur lors de l'inspection: " + e.getMessage());
            return result;
        }
    }
    
    /**
     * Compte le nombre de lignes dans un fichier Parquet
     * 
     * @param filePath Le chemin du fichier Parquet
     * @param useFastCount Si true, utilise l'estimation rapide depuis les métadonnées,
     *                   sinon compte précisément en parcourant le fichier
     * @return Le nombre de lignes, ou -1 en cas d'erreur
     */
    public long countRowsInParquetFile(String filePath, boolean useFastCount) {
        if (!isValidParquetFile(filePath)) {
            return -1;
        }
        
        try {
            Configuration conf = createHadoopConfiguration();
            Path path = new Path(filePath);
            
            if (useFastCount) {
                // Méthode rapide: estimer à partir des métadonnées
                ParquetMetadata metadata = ParquetFileReader.readFooter(conf, path);
                return metadata.getBlocks().stream()
                        .mapToLong(block -> block.getRowCount())
                        .sum();
            } else {
                // Méthode précise: compter chaque ligne
                long rowCount = 0;
                try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(
                        HadoopInputFile.fromPath(path, conf))
                        .withConf(conf)
                        .build()) {
                    
                    while (reader.read() != null) {
                        rowCount++;
                        
                        // Afficher la progression pour les grands fichiers
                        if (rowCount % 100000 == 0) {
                            logger.info("Comptage des lignes: " + rowCount + " lignes traitées jusqu'à présent");
                        }
                    }
                }
                
                return rowCount;
            }
        } catch (IOException e) {
            logger.severe("Erreur lors du comptage des lignes: " + e.getMessage());
            return -1;
        }
    }
    
    /**
     * Converti un type Parquet en chaîne de caractères lisible
     */
    private String convertParquetTypeToString(Type type) {
        if (type.isPrimitive()) {
            PrimitiveType primitiveType = type.asPrimitiveType();
            return primitiveType.getPrimitiveTypeName().name();
        } else {
            return type.toString();
        }
    }
    
    /**
     * Crée une configuration Hadoop optimisée
     */
    private Configuration createHadoopConfiguration() {
        Configuration configuration = new Configuration();
        
        // Désactiver l'authentification et l'autorisation
        configuration.set("hadoop.security.authentication", "simple");
        configuration.set("hadoop.security.authorization", "false");
        configuration.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        configuration.set("fs.file.impl.disable.cache", "true");
        configuration.set("parquet.strings.signed-min-max.enabled", "false");
        
        // Optimisations de lecture
        configuration.setInt("parquet.block.size", 128 * 1024 * 1024); // 128 MB
        configuration.setInt("parquet.page.size", 1 * 1024 * 1024);    // 1 MB
        configuration.setBoolean("parquet.enable.dictionary", true);
        
        return configuration;
    }
} 