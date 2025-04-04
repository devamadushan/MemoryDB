package com.ARYD.MemoryDB.service;

import com.ARYD.MemoryDB.storage.Database;
import com.ARYD.MemoryDB.storage.Table;
import com.ARYD.MemoryDB.storage.ColumnDefinition;
import com.ARYD.MemoryDB.types.DataTypeFactory;
import com.ARYD.network.ClusterManager;
import com.ARYD.network.ServerNode;
import com.ARYD.network.ClusterConfig;
import com.ARYD.entity.Record;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.springframework.stereotype.Service;
import com.ARYD.MemoryDB.entity.DataFrame;
import com.ARYD.MemoryDB.types.DataType;
import com.ARYD.MemoryDB.types.DataTypeMapper;
import com.ARYD.MemoryDB.query.QueryEngine;
import jakarta.annotation.PostConstruct;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

@Service
@Slf4j
public class ParquetService {
    private final Database database;
    private final ClusterManager clusterManager;
    private final TableService tableService;
    private ExecutorService executorService;
    
    // Track loading statistics
    private Map<String, AtomicInteger> loadingStats = new ConcurrentHashMap<>();

    public ParquetService(Database database, ClusterManager clusterManager, TableService tableService) {
        this.database = database;
        this.clusterManager = clusterManager;
        this.tableService = tableService;
        this.executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
    }

    @PostConstruct
    private void init() {
        // Configuration pour Java 17+ permettant l'accès sans SecurityManager
        System.setProperty("hadoop.home.dir", "/");
        
        // Ces propriétés permettent d'éviter l'erreur "getSubject is supported only if a security manager is allowed"
        System.setProperty("java.security.manager", "allow");
        System.setProperty("HADOOP_USER_NAME", "hadoop");
        
        log.info("Hadoop configuration initialized for Java 17+ compatibility");
    }

    /**
     * Lit un fichier Parquet et le convertit en Table optimisée
     * @param filePath Chemin du fichier Parquet
     * @param maxRows Nombre maximum de lignes à charger (optionnel, -1 pour tout charger)
     * @return Table contenant les données du fichier Parquet
     */
    public Table readParquetFile(String filePath, int maxRows) {
        return readParquetFile(filePath, maxRows, null);
    }
    
    /**
     * Lit un fichier Parquet et le convertit en Table optimisée avec un nom personnalisé
     * @param filePath Chemin du fichier Parquet
     * @param maxRows Nombre maximum de lignes à charger (optionnel, -1 pour tout charger)
     * @param customTableName Nom personnalisé pour la table (optionnel)
     * @return Table contenant les données du fichier Parquet
     */
    public Table readParquetFile(String filePath, int maxRows, String customTableName) {
        log.info("Début de la lecture du fichier Parquet: {}, maxRows: {}", filePath, maxRows == -1 ? "illimité" : maxRows);
        if (customTableName != null && !customTableName.isEmpty()) {
            log.info("Utilisation du nom de table personnalisé: {}", customTableName);
        }
        
        // Vérifier que le fichier existe
        File file = new File(filePath);
        if (!file.exists() || !file.isFile()) {
            log.error("Le fichier Parquet n'existe pas ou n'est pas un fichier valide: {}", filePath);
            return null;
        }
        
        log.info("Taille du fichier: {} bytes", file.length());
        
        Path path = new Path(filePath);
        Configuration configuration = new Configuration();
        
        // Désactiver l'authentification et l'autorisation de sécurité Hadoop
        configuration.set("hadoop.security.authentication", "simple");
        configuration.set("hadoop.security.authorization", "false");
        configuration.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        configuration.set("fs.file.impl.disable.cache", "true");
        configuration.set("parquet.strings.signed-min-max.enabled", "false");

        try {
            log.debug("Tentative d'ouverture du fichier Parquet avec AvroParquetReader...");
            
            // Obtenir des statistiques sur le fichier Parquet
            ParquetMetadata metadata = ParquetFileReader.readFooter(configuration, path);
            long totalRowGroups = metadata.getBlocks().size();
            long estimatedRows = metadata.getBlocks().stream()
                    .mapToLong(block -> block.getRowCount())
                    .sum();
            
            log.info("Métadonnées du fichier: {} groupes de lignes, environ {} lignes estimées, créé par: {}", 
                     totalRowGroups, estimatedRows, metadata.getFileMetaData().getCreatedBy());
            
            // Créer un lecteur Parquet
            ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(
                     HadoopInputFile.fromPath(path, configuration))
                     .withConf(configuration)
                     .build();
            
            log.debug("Lecture du premier enregistrement pour obtenir le schéma...");
            // Lire le premier enregistrement pour obtenir le schéma
            GenericRecord firstRecord = reader.read();
            if (firstRecord == null) {
                log.warn("Le fichier Parquet est vide: {}", filePath);
                reader.close();
                return null;
            }

            log.debug("Premier enregistrement lu avec succès, création de la table...");
            // Créer la table avec le bon nom
            String tableName = customTableName;
            if (tableName == null || tableName.isEmpty()) {
                tableName = path.getName().replace(".parquet", "");
                log.info("Utilisation du nom par défaut basé sur le fichier: {}", tableName);
            }
            
            List<ColumnDefinition> columns = new ArrayList<>();

            // Analyser le schéma et créer les colonnes avec les types appropriés
            Schema schema = firstRecord.getSchema();
            log.debug("Schéma détecté avec {} champs", schema.getFields().size());
            
            for (Schema.Field field : schema.getFields()) {
                String columnName = field.name();
                Schema fieldSchema = field.schema();
                
                // Gérer les types union (union avec null représente une colonne nullable)
                String typeName;
                boolean nullable = false;
                
                if (fieldSchema.getType() == Schema.Type.UNION) {
                    // Déterminer le type réel dans l'union
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
                
                boolean indexed = false;

                log.debug("Ajout de la colonne: {}, type: {}, nullable: {}", columnName, typeName, nullable);
                columns.add(ColumnDefinition.of(columnName, typeName, nullable, indexed));
            }

            // Créer la table dans la base de données
            log.debug("Création de la table {} avec {} colonnes...", tableName, columns.size());
            Table table = database.createTable(tableName, columns);
            if (table == null) {
                log.error("Échec de la création de la table dans la base de données");
                reader.close();
                return null;
            }

            // Ajouter le premier enregistrement
            log.debug("Ajout du premier enregistrement à la table...");
            addRecordToTable(table, firstRecord);

            // Lire les enregistrements restants en parallèle
            List<GenericRecord> batch = new ArrayList<>();
            int batchSize = 10000; // Taille du batch pour le traitement parallèle
            int rowCount = 1;
            long startTime = System.currentTimeMillis();
            long lastReportTime = startTime;

            log.info("Début du chargement des données. Total estimé: environ {} lignes{}", 
                    estimatedRows, maxRows > 0 ? ", limité à " + maxRows + " lignes" : "");
            log.debug("Début de la lecture des enregistrements restants...");
            GenericRecord record;
            int batchCount = 0;
            
            while ((record = reader.read()) != null) {
                batch.add(record);
                rowCount++;

                // Vérifier si on a atteint la limite de lignes
                if (maxRows > 0 && rowCount >= maxRows) {
                    log.info("Limite de {} lignes atteinte, arrêt du chargement", maxRows);
                    break;
                }

                if (batch.size() >= batchSize) {
                    batchCount++;
                    log.debug("Traitement du batch #{} avec {} enregistrements...", batchCount, batch.size());
                    processBatch(table, batch);
                    batch.clear();
                    
                    // Log périodique pour montrer la progression
                    long currentTime = System.currentTimeMillis();
                    if (currentTime - lastReportTime > 5000) { // Rapport toutes les 5 secondes
                        double percentComplete;
                        if (maxRows > 0) {
                            percentComplete = (double)rowCount / maxRows * 100.0;
                        } else {
                            percentComplete = (double)rowCount / estimatedRows * 100.0;
                        }
                        double rowsPerSecond = rowCount * 1000.0 / (currentTime - startTime);
                        log.info("Progression: {} lignes traitées ({:.2f}%), vitesse: {:.0f} lignes/sec", 
                                rowCount, percentComplete, rowsPerSecond);
                        lastReportTime = currentTime;
                    }
                }
            }

            // Traiter le dernier batch s'il n'est pas vide
            if (!batch.isEmpty()) {
                batchCount++;
                log.debug("Traitement du dernier batch #{} avec {} enregistrements...", batchCount, batch.size());
                processBatch(table, batch);
            }

            // S'assurer que toutes les tâches soumises au ExecutorService sont terminées
            log.info("Attente de la fin de toutes les tâches en cours...");
            try {
                // Attendre que toutes les tâches soumises soient terminées (max 5 minutes)
                executorService.shutdown();
                boolean terminated = executorService.awaitTermination(5, TimeUnit.MINUTES);
                if (!terminated) {
                    log.warn("L'exécution des tâches a dépassé le délai de 5 minutes, certaines tâches pourraient être abandonnées");
                }
            } catch (InterruptedException e) {
                log.error("Interruption pendant l'attente de la fin des tâches", e);
                Thread.currentThread().interrupt();
            } finally {
                // Recréer l'ExecutorService pour les opérations futures
                executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
            }

            long endTime = System.currentTimeMillis();
            double elapsedSeconds = (endTime - startTime) / 1000.0;
            log.info("Lecture du fichier Parquet terminée avec succès. {} lignes lues en {:.2f} secondes ({:.0f} lignes/sec)", 
                    rowCount, elapsedSeconds, rowCount / elapsedSeconds);
            reader.close();
            
            // Vérifier le nombre final de lignes
            int tableRowCount = table.getRowCount();
            if (tableRowCount != rowCount) {
                log.warn("Différence entre le nombre de lignes lues ({}) et le nombre de lignes dans la table ({})", 
                        rowCount, tableRowCount);
            }
            
            // Indiquer si le fichier a potentiellement plus de lignes (uniquement si on a atteint la limite)
            if (maxRows > 0 && rowCount >= maxRows) {
                log.info("Le fichier contient probablement plus de lignes que la limite de {} spécifiée", maxRows);
            }
            
            // Create a DataFrame from the Table for SQL queries
            DataFrame dataFrame = createDataFrameFromTable(table);
            tableService.addTable(dataFrame);
            
            return table;

        } catch (Exception e) {
            log.error("Erreur lors de la lecture du fichier Parquet: {} - {}", filePath, e.getMessage(), e);
            return null;
        }
    }

    /**
     * Lit un fichier Parquet et le convertit en Table optimisée (charge toutes les lignes)
     * @param filePath Chemin du fichier Parquet
     * @return Table contenant les données du fichier Parquet
     */
    public Table readParquetFile(String filePath) {
        return readParquetFile(filePath, -1);
    }

    private void processBatch(Table table, List<GenericRecord> batch) {
        try {
            log.debug("Début du traitement d'un batch de {} enregistrements", batch.size());
            
            // Traiter le batch en parallèle avec des tâches soumises à l'ExecutorService
            List<Runnable> tasks = new ArrayList<>();
            for (GenericRecord record : batch) {
                tasks.add(() -> addRecordToTable(table, record));
            }
            
            // Exécuter les tâches en parallèle et les collecter dans une liste
            List<java.util.concurrent.Future<?>> futures = new ArrayList<>();
            for (Runnable task : tasks) {
                futures.add(executorService.submit(task));
            }
            
            // Attendre que toutes les tâches soient terminées
            for (java.util.concurrent.Future<?> future : futures) {
                try {
                    future.get(); // Bloque jusqu'à ce que la tâche soit terminée
                } catch (Exception e) {
                    log.error("Erreur lors de l'exécution d'une tâche: {}", e.getMessage(), e);
                }
            }
            
            log.debug("Fin du traitement du batch - {} enregistrements traités", batch.size());
        } catch (Exception e) {
            log.error("Erreur lors du traitement d'un batch d'enregistrements: {}", e.getMessage(), e);
            throw e;
        }
    }

    /**
     * Convertit un type Avro en type de données utilisé par notre système.
     * 
     * @param avroType le type Avro à convertir
     * @return le nom du type de données compatible avec DataTypeFactory
     */
    private String convertAvroTypeToDataType(String avroType) {
        // Les types de données doivent correspondre aux constantes définies dans DataTypeFactory
        switch (avroType.toUpperCase()) {
            case "STRING":
                return "STRING"; // Auparavant "StringType"
            case "INT":
            case "INTEGER":
                return "INT"; // Auparavant "IntType"
            case "LONG":
                return "LONG"; // Auparavant "LongType"
            case "FLOAT":
                return "FLOAT"; // Auparavant "FloatType"
            case "DOUBLE":
                return "DOUBLE"; // Auparavant "DoubleType"
            case "BOOLEAN":
                return "BOOLEAN"; // Auparavant "BooleanType"
            case "BINARY":
                return "BINARY"; // Auparavant "BinaryType"
            case "DATE":
                return "DATE"; // Auparavant "DateType"
            case "TIMESTAMP":
            case "TIMESTAMP_MILLIS":
                return "TIMESTAMP"; // Auparavant "TimestampType"
            default:
                log.warn("Type Avro inconnu: {}. Utilisation du type STRING par défaut.", avroType);
                return "STRING"; // Type par défaut si inconnu
        }
    }

    /**
     * Ajoute un enregistrement Parquet à la table
     */
    private void addRecordToTable(Table table, GenericRecord record) {
        try {
            for (Schema.Field field : record.getSchema().getFields()) {
                String columnName = field.name();
                Object value = record.get(columnName);
                table.addValue(columnName, value);
            }
        } catch (Exception e) {
            log.error("Erreur lors de l'ajout d'un enregistrement à la table: {}", e.getMessage(), e);
            throw e;
        }
    }

    /**
     * Creates a DataFrame from a Table to enable SQL queries
     */
    private DataFrame createDataFrameFromTable(Table table) {
        // Create a new DataFrame
        DataFrame df = new DataFrame();
        df.setTableName(table.getName());
        
        // Get all column names from the table
        List<String> columnNames = table.getColumnNames();
        
        // Add each column to the DataFrame
        for (String colName : columnNames) {
            df.addColumn(colName);
        }
        
        // Add each row from the table to the DataFrame
        for (int i = 0; i < table.getRowCount(); i++) {
            Map<String, Object> row = table.getRow(i);
            df.addRow(row);
        }
        
        return df;
    }

    /**
     * Load Parquet file with round-robin distribution across cluster nodes
     */
    public void loadParquetWithRoundRobin(String filePath, String tableName, int maxRows, String loadingId) {
        log.info("Starting round-robin distributed loading for table: {}, file: {}", tableName, filePath);
        
        // Get active nodes in the cluster
        List<ServerNode> nodes = clusterManager.getActiveNodes();
        if (nodes.isEmpty()) {
            log.warn("No active nodes found in cluster, loading data locally only");
            nodes.add(ClusterConfig.getInstance().getCurrentNode());
        }
        
        // Initialize loading stats
        loadingStats.put(loadingId, new AtomicInteger(0));
        
        try {
            Path path = new Path(filePath);
            Configuration configuration = new Configuration();
            
            // Configure Hadoop properties
            configureHadoop(configuration);
            
            // Create Parquet reader
            ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(
                    HadoopInputFile.fromPath(path, configuration))
                    .withConf(configuration)
                    .build();
            
            GenericRecord record;
            int rowIndex = 0;
            
            log.info("Beginning round-robin distribution across {} nodes", nodes.size());
            
            // Read and distribute records
            while ((record = reader.read()) != null && (maxRows == -1 || rowIndex < maxRows)) {
                int targetNodeIndex = rowIndex % nodes.size();
                ServerNode targetNode = nodes.get(targetNodeIndex);
                
                if (ClusterConfig.getInstance().isCurrentNode(targetNode.getName())) {
                    // Local node - add directly
                    Table table = database.getTable(tableName);
                    if (table != null) {
                        addRecordToTable(table, record);
                        loadingStats.get(loadingId).incrementAndGet();
                    } else {
                        log.error("Table not found: {}", tableName);
                    }
                } else {
                    // Remote node - send record
                    sendRecordToNode(targetNode, tableName, record, loadingId);
                }
                
                // Progress logging
                if (rowIndex > 0 && rowIndex % 10000 == 0) {
                    log.info("Processed {} rows for distributed loading {}", rowIndex, loadingId);
                }
                
                rowIndex++;
            }
            
            reader.close();
            log.info("Completed round-robin distribution, processed {} records", rowIndex);
            
        } catch (Exception e) {
            log.error("Error in round-robin distribution", e);
            throw new RuntimeException("Error in round-robin distribution: " + e.getMessage(), e);
        }
    }
    
    /**
     * Load Parquet file with hash-based distribution across cluster nodes
     */
    public void loadParquetWithHashDistribution(String filePath, String tableName, 
                                               int maxRows, String distributionColumn, String loadingId) {
        log.info("Starting hash-based distributed loading for table: {}, file: {}, column: {}", 
                tableName, filePath, distributionColumn);
        
        // Get active nodes in the cluster
        List<ServerNode> nodes = clusterManager.getActiveNodes();
        if (nodes.isEmpty()) {
            log.warn("No active nodes found in cluster, loading data locally only");
            nodes.add(ClusterConfig.getInstance().getCurrentNode());
        }
        
        // Initialize loading stats
        loadingStats.put(loadingId, new AtomicInteger(0));
        
        try {
            Path path = new Path(filePath);
            Configuration configuration = new Configuration();
            
            // Configure Hadoop properties
            configureHadoop(configuration);
            
            // Create Parquet reader
            ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(
                    HadoopInputFile.fromPath(path, configuration))
                    .withConf(configuration)
                    .build();
            
            GenericRecord record;
            int rowIndex = 0;
            
            log.info("Beginning hash-based distribution across {} nodes", nodes.size());
            
            // Read and distribute records
            while ((record = reader.read()) != null && (maxRows == -1 || rowIndex < maxRows)) {
                // Calculate hash based on the distribution column
                Object columnValue = record.get(distributionColumn);
                int targetNodeIndex;
                
                if (columnValue != null) {
                    // Use hash code to determine target node
                    int hashCode = columnValue.hashCode();
                    targetNodeIndex = Math.abs(hashCode % nodes.size());
                } else {
                    // Fall back to round-robin if column value is null
                    targetNodeIndex = rowIndex % nodes.size();
                }
                
                ServerNode targetNode = nodes.get(targetNodeIndex);
                
                if (ClusterConfig.getInstance().isCurrentNode(targetNode.getName())) {
                    // Local node - add directly
                    Table table = database.getTable(tableName);
                    if (table != null) {
                        addRecordToTable(table, record);
                        loadingStats.get(loadingId).incrementAndGet();
                    } else {
                        log.error("Table not found: {}", tableName);
                    }
                } else {
                    // Remote node - send record
                    sendRecordToNode(targetNode, tableName, record, loadingId);
                }
                
                // Progress logging
                if (rowIndex > 0 && rowIndex % 10000 == 0) {
                    log.info("Processed {} rows for distributed loading {}", rowIndex, loadingId);
                }
                
                rowIndex++;
            }
            
            reader.close();
            log.info("Completed hash-based distribution, processed {} records", rowIndex);
            
        } catch (Exception e) {
            log.error("Error in hash-based distribution", e);
            throw new RuntimeException("Error in hash-based distribution: " + e.getMessage(), e);
        }
    }
    
    /**
     * Send a record to a remote node in the cluster
     */
    private void sendRecordToNode(ServerNode node, String tableName, GenericRecord record, String loadingId) {
        try {
            // Convert GenericRecord to a Map
            Map<String, Object> recordMap = new HashMap<>();
            for (Schema.Field field : record.getSchema().getFields()) {
                String fieldName = field.name();
                Object value = record.get(fieldName);
                recordMap.put(fieldName, value);
            }
            
            // Create record object
            Record recordData = new Record();
            recordData.setTableName(tableName);
            recordData.setData(recordMap);
            recordData.setLoadingId(loadingId);
            
            // Send record to remote node
            boolean success = clusterManager.sendRecord(node, recordData);
            
            if (success) {
                loadingStats.get(loadingId).incrementAndGet();
            }
        } catch (Exception e) {
            log.error("Error sending record to node: {}", node.getName(), e);
        }
    }
    
    /**
     * Get the total number of rows loaded for a specific loading operation
     */
    public int getTotalRowsLoaded(String loadingId) {
        AtomicInteger counter = loadingStats.get(loadingId);
        return counter != null ? counter.get() : 0;
    }
    
    /**
     * Configure Hadoop properties for Parquet reading
     */
    private void configureHadoop(Configuration configuration) {
        // Disable authentication and Hadoop security
        configuration.set("hadoop.security.authentication", "simple");
        configuration.set("hadoop.security.authorization", "false");
        configuration.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        configuration.set("fs.file.impl.disable.cache", "true");
        configuration.set("parquet.strings.signed-min-max.enabled", "false");
    }
}