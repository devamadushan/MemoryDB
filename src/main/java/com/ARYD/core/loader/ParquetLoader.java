package com.ARYD.core.loader;

import com.ARYD.core.storage.Table;
import com.ARYD.core.storage.Database;
import com.ARYD.core.storage.ColumnDefinition;
import com.ARYD.core.types.DataType;
import com.ARYD.entity.LoadingStatus;
import com.ARYD.exception.ARYDException;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * Chargeur optimisé pour les fichiers Parquet
 */
@Singleton
public class ParquetLoader {

    private static final Logger logger = Logger.getLogger(ParquetLoader.class.getName());
    private static final int DEFAULT_BATCH_SIZE = 10000;
    
    private final Database database;
    private ExecutorService executorService;

    @Inject
    public ParquetLoader(Database database) {
        this.database = database;
        this.executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        
        // Configuration Hadoop pour Java 17+
        System.setProperty("hadoop.home.dir", "/");
        System.setProperty("java.security.manager", "allow");
        System.setProperty("HADOOP_USER_NAME", "hadoop");
        
        logger.info("ParquetLoader initialized avec " + Runtime.getRuntime().availableProcessors() + " threads");
    }

    /**
     * Charge un fichier Parquet dans une Table
     * 
     * @param filePath Chemin du fichier Parquet
     * @param maxRows Nombre maximum de lignes à charger (-1 pour toutes)
     * @param status Objet de statut pour le suivi de progression (peut être null)
     * @return Table créée ou null en cas d'erreur
     */
    public Table loadParquetFile(String filePath, int maxRows, LoadingStatus status) {
        logger.info("Début de la lecture du fichier Parquet: " + filePath + ", maxRows: " + (maxRows == -1 ? "illimité" : maxRows));
        
        // Vérifier que le fichier existe
        File file = new File(filePath);
        if (!file.exists() || !file.isFile()) {
            logger.severe("Le fichier Parquet n'existe pas ou n'est pas un fichier valide: " + filePath);
            return null;
        }
        
        logger.info("Taille du fichier: " + file.length() + " bytes");
        
        Path path = new Path(filePath);
        Configuration configuration = createHadoopConfiguration();

        try {
            // Obtenir des statistiques sur le fichier Parquet
            ParquetMetadata metadata = ParquetFileReader.readFooter(configuration, path);
            long totalRowGroups = metadata.getBlocks().size();
            long estimatedRows = metadata.getBlocks().stream()
                    .mapToLong(block -> block.getRowCount())
                    .sum();
            
            logger.info("Métadonnées du fichier: " + totalRowGroups + " groupes de lignes, environ " 
                    + estimatedRows + " lignes estimées, créé par: " + metadata.getFileMetaData().getCreatedBy());
            
            if (status != null) {
                status.setStatus("ANALYZING");
            }
            
            // Créer un lecteur Parquet
            try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(
                     HadoopInputFile.fromPath(path, configuration))
                     .withConf(configuration)
                     .build()) {
            
                // Lire le premier enregistrement pour obtenir le schéma
                GenericRecord firstRecord = reader.read();
                if (firstRecord == null) {
                    logger.warning("Le fichier Parquet est vide: " + filePath);
                    return null;
                }

                // Créer la table avec le bon nom
                String tableName = path.getName().replace(".parquet", "");
                List<ColumnDefinition> columns = extractColumnsFromSchema(firstRecord.getSchema());

                // Créer la table dans la base de données
                Table table = database.createTable(tableName, columns);
                
                // Configurer le chargement
                int batchSize = DEFAULT_BATCH_SIZE; 
                int rowCount = 1;
                long startTime = System.currentTimeMillis();
                long lastReportTime = startTime;
                
                if (status != null) {
                    status.setStatus("LOADING");
                }
                
                // Ajouter le premier enregistrement
                addRecordToTable(table, firstRecord);
                
                // Lire les enregistrements restants par batch
                List<GenericRecord> batch = new ArrayList<>();
                GenericRecord record;
                int batchCount = 0;
                
                logger.info("Début du chargement. Estimation: " + estimatedRows + " lignes" + 
                           (maxRows > 0 ? ", limité à " + maxRows + " lignes" : ""));
                
                while ((record = reader.read()) != null) {
                    batch.add(record);
                    rowCount++;

                    // Vérifier si on a atteint la limite
                    if (maxRows > 0 && rowCount >= maxRows) {
                        logger.info("Limite de " + maxRows + " lignes atteinte");
                        break;
                    }

                    // Traiter les batches complets
                    if (batch.size() >= batchSize) {
                        batchCount++;
                        logger.fine("Traitement du batch #" + batchCount + " (" + batch.size() + " lignes)");
                        processBatch(table, batch);
                        batch.clear();
                        
                        // Mettre à jour la progression
                        updateProgress(status, rowCount, maxRows > 0 ? maxRows : estimatedRows);
                        
                        // Log périodique de progression
                        long currentTime = System.currentTimeMillis();
                        if (currentTime - lastReportTime > 5000) { // Toutes les 5 secondes
                            double percentage = calculatePercentage(rowCount, maxRows, estimatedRows);
                            double rowsPerSecond = rowCount * 1000.0 / (currentTime - startTime);
                            logger.info(String.format("Progression: %d lignes (%.2f%%), vitesse: %.0f lignes/sec", 
                                    rowCount, percentage, rowsPerSecond));
                            lastReportTime = currentTime;
                        }
                    }
                }

                // Traiter le dernier batch s'il n'est pas vide
                if (!batch.isEmpty()) {
                    batchCount++;
                    logger.fine("Traitement du dernier batch #" + batchCount + " (" + batch.size() + " lignes)");
                    processBatch(table, batch);
                }

                // S'assurer que toutes les tâches sont terminées
                waitForCompletion();
                
                // Finaliser
                long endTime = System.currentTimeMillis();
                double seconds = (endTime - startTime) / 1000.0;
                logger.info(String.format("Chargement terminé: %d lignes en %.2f secondes (%.0f lignes/sec)", 
                        rowCount, seconds, rowCount / seconds));
                
                if (status != null) {
                    status.setRowCount(rowCount);
                    status.setProgress(100);
                }
                
                return table;
            }
        } catch (Exception e) {
            logger.severe("Erreur lors du chargement du fichier Parquet: " + e.getMessage());
            throw new ARYDException("Erreur lors du chargement du fichier Parquet: " + e.getMessage(), 
                    e, "PARQUET_LOAD_ERROR");
        }
    }
    
    /**
     * Charge un fichier Parquet complet (sans limite de lignes)
     */
    public Table loadParquetFile(String filePath) {
        return loadParquetFile(filePath, -1, null);
    }
    
    /**
     * Traite un batch d'enregistrements en mode parallèle
     */
    private void processBatch(Table table, List<GenericRecord> batch) {
        try {
            // Traiter le batch en parallèle
            List<Future<?>> futures = new ArrayList<>();
            
            for (GenericRecord record : batch) {
                futures.add(executorService.submit(() -> addRecordToTable(table, record)));
            }
            
            // Attendre que toutes les tâches soient terminées
            for (Future<?> future : futures) {
                try {
                    future.get(); // Bloque jusqu'à ce que la tâche soit terminée
                } catch (Exception e) {
                    logger.warning("Erreur dans une tâche: " + e.getMessage());
                }
            }
        } catch (Exception e) {
            logger.severe("Erreur lors du traitement d'un batch: " + e.getMessage());
            throw new ARYDException("Erreur lors du traitement d'un batch: " + e.getMessage(), 
                    e, "BATCH_PROCESS_ERROR");
        }
    }
    
    /**
     * Ajoute un enregistrement à la table
     */
    private void addRecordToTable(Table table, GenericRecord record) {
        try {
            for (Schema.Field field : record.getSchema().getFields()) {
                String columnName = field.name();
                Object value = record.get(columnName);
                table.addValue(columnName, value);
            }
        } catch (Exception e) {
            logger.warning("Erreur lors de l'ajout d'un enregistrement: " + e.getMessage());
            throw new ARYDException("Erreur lors de l'ajout d'un enregistrement: " + e.getMessage(), 
                    e, "RECORD_ADD_ERROR");
        }
    }
    
    /**
     * Extrait les définitions de colonnes depuis un schéma Avro
     */
    private List<ColumnDefinition> extractColumnsFromSchema(Schema schema) {
        List<ColumnDefinition> columns = new ArrayList<>();
        
        for (Schema.Field field : schema.getFields()) {
            String columnName = field.name();
            Schema fieldSchema = field.schema();
            
            String typeName;
            boolean nullable = false;
            
            // Gérer les types union (nullable)
            if (fieldSchema.getType() == Schema.Type.UNION) {
                List<Schema> types = fieldSchema.getTypes();
                Schema actualType = null;
                
                for (Schema type : types) {
                    if (type.getType() != Schema.Type.NULL) {
                        actualType = type;
                        nullable = true;
                    }
                }
                
                if (actualType != null) {
                    typeName = convertAvroTypeToDataType(actualType.getType().toString());
                } else {
                    typeName = "STRING"; // Par défaut
                }
            } else {
                typeName = convertAvroTypeToDataType(fieldSchema.getType().toString());
            }
            
            // Déterminer si la colonne doit être indexée
            boolean indexed = shouldBeIndexed(columnName, typeName);
            
            logger.fine("Colonne détectée: " + columnName + ", type: " + typeName + ", nullable: " + nullable);
            columns.add(new ColumnDefinition(columnName, typeName, nullable, indexed));
        }
        
        return columns;
    }
    
    /**
     * Détermine si une colonne devrait être indexée automatiquement
     */
    private boolean shouldBeIndexed(String columnName, String typeName) {
        // Règles heuristiques pour décider quelles colonnes indexer
        
        // Les colonnes d'ID sont généralement bonnes candidates pour l'indexation
        if (columnName.toLowerCase().endsWith("id") && 
            (typeName.equals("INT") || typeName.equals("LONG") || typeName.equals("STRING"))) {
            return true;
        }
        
        // Colonnes de date souvent utilisées pour filtrage
        if (typeName.equals("DATE") || typeName.equals("TIMESTAMP")) {
            return true;
        }
        
        // Les colonnes booléennes avec peu de valeurs distinctes sont efficaces à indexer
        if (typeName.equals("BOOLEAN")) {
            return true;
        }
        
        // Par défaut, pas d'indexation
        return false;
    }
    
    /**
     * Convertit un type Avro en type de données interne
     */
    private String convertAvroTypeToDataType(String avroType) {
        // Les types de données doivent correspondre aux constantes définies dans DataType
        switch (avroType.toUpperCase()) {
            case "STRING":
                return "STRING";
            case "INT":
            case "INTEGER":
                return "INT";
            case "LONG":
                return "LONG";
            case "FLOAT":
                return "FLOAT";
            case "DOUBLE":
                return "DOUBLE";
            case "BOOLEAN":
                return "BOOLEAN";
            case "BINARY":
                return "BINARY";
            case "DATE":
                return "DATE";
            case "TIMESTAMP":
            case "TIMESTAMP_MILLIS":
                return "TIMESTAMP";
            default:
                logger.warning("Type Avro inconnu: " + avroType + ". Utilisation du type STRING par défaut.");
                return "STRING";
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
        configuration.setBoolean("parquet.enable.column.index", true);
        
        return configuration;
    }
    
    /**
     * Attend que toutes les tâches soient terminées
     */
    private void waitForCompletion() {
        try {
            logger.info("Attente de la fin de tous les traitements en cours...");
            
            // Arrêter l'acceptation de nouvelles tâches mais continuer celles en cours
            executorService.shutdown();
            
            // Attendre la fin de toutes les tâches (max 5 minutes)
            boolean terminated = executorService.awaitTermination(5, TimeUnit.MINUTES);
            if (!terminated) {
                logger.warning("Certaines tâches n'ont pas pu se terminer dans le délai imparti (5min)");
            }
        } catch (InterruptedException e) {
            logger.warning("Interruption pendant l'attente de fin des tâches: " + e.getMessage());
            Thread.currentThread().interrupt();
        } finally {
            // Recréer l'ExecutorService pour les opérations futures
            executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        }
    }
    
    /**
     * Calcule le pourcentage de progression
     */
    private double calculatePercentage(int rowCount, int maxRows, long estimatedRows) {
        if (maxRows > 0) {
            return Math.min(100.0, rowCount * 100.0 / maxRows);
        } else {
            return Math.min(100.0, rowCount * 100.0 / estimatedRows);
        }
    }
    
    /**
     * Met à jour la progression dans l'objet de statut
     */
    private void updateProgress(LoadingStatus status, int currentRows, long totalRows) {
        if (status != null) {
            double percentage = Math.min(99.0, currentRows * 100.0 / totalRows); // Max 99% jusqu'à complétion
            status.setProgress((int) percentage);
            status.setRowCount(currentRows);
        }
    }
} 
 