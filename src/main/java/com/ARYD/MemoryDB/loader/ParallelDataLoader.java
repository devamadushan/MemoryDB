package com.ARYD.MemoryDB.loader;

import com.ARYD.MemoryDB.storage.Table;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

/**
 * Chargeur de données parallèle pour importer rapidement des données dans les tables
 */
public class ParallelDataLoader {

    private static final int DEFAULT_THREAD_COUNT = Runtime.getRuntime().availableProcessors();
    private static final int DEFAULT_BATCH_SIZE = 10000;
    
    private final int threadCount;
    private final int batchSize;
    private final ExecutorService executorService;
    
    /**
     * Constructeur avec nombre de threads et taille de lot par défaut
     */
    public ParallelDataLoader() {
        this(DEFAULT_THREAD_COUNT, DEFAULT_BATCH_SIZE);
    }
    
    /**
     * Constructeur avec paramètres personnalisés
     * @param threadCount Nombre de threads à utiliser
     * @param batchSize Taille des lots de données à traiter
     */
    public ParallelDataLoader(int threadCount, int batchSize) {
        this.threadCount = threadCount;
        this.batchSize = batchSize;
        this.executorService = Executors.newFixedThreadPool(threadCount);
    }
    
    /**
     * Charge des données CSV dans une table
     * @param table Table cible
     * @param csvFile Fichier CSV source
     * @param hasHeader Indique si le fichier a une ligne d'en-tête
     * @throws IOException En cas d'erreur d'E/S
     */
    public void loadFromCSV(Table table, Path csvFile, boolean hasHeader) throws IOException, ExecutionException, InterruptedException {
        if (!Files.exists(csvFile)) {
            throw new FileNotFoundException("Fichier introuvable: " + csvFile);
        }
        
        // Lire les en-têtes si présents
        List<String> headers;
        try (BufferedReader reader = Files.newBufferedReader(csvFile)) {
            String headerLine = reader.readLine();
            if (headerLine == null) {
                throw new IOException("Fichier vide");
            }
            
            if (hasHeader) {
                headers = Arrays.asList(headerLine.split(","));
            } else {
                // Si pas d'en-tête, utiliser les noms de colonnes de la table
                headers = table.getColumnNames();
                // Revenir au début du fichier
                reader.close();
            }
        }
        
        // Vérifier la correspondance des colonnes
        List<String> tableColumns = table.getColumnNames();
        for (String header : headers) {
            if (!tableColumns.contains(header)) {
                throw new IllegalArgumentException("La colonne '" + header + "' n'existe pas dans la table");
            }
        }
        
        // Compter les lignes pour estimation
        long lineCount = Files.lines(csvFile).count();
        if (hasHeader) {
            lineCount--;
        }
        
        System.out.println("Chargement de " + lineCount + " lignes avec " + threadCount + " threads");
        
        // Diviser le fichier en lots pour traitement parallèle
        List<Future<Integer>> futures = new ArrayList<>();
        
        try (BufferedReader reader = Files.newBufferedReader(csvFile)) {
            // Ignorer l'en-tête si présent
            if (hasHeader) {
                reader.readLine();
            }
            
            int startLine = 0;
            List<String> batch = new ArrayList<>(batchSize);
            String line;
            
            while ((line = reader.readLine()) != null) {
                batch.add(line);
                
                if (batch.size() >= batchSize) {
                    List<String> currentBatch = new ArrayList<>(batch);
                    Future<Integer> future = executorService.submit(() -> processBatch(table, currentBatch, headers));
                    futures.add(future);
                    
                    batch.clear();
                    startLine += batchSize;
                }
            }
            
            // Traiter le dernier lot s'il reste des lignes
            if (!batch.isEmpty()) {
                Future<Integer> future = executorService.submit(() -> processBatch(table, batch, headers));
                futures.add(future);
            }
        }
        
        // Attendre la fin de tous les traitements
        int totalRows = 0;
        for (Future<Integer> future : futures) {
            totalRows += future.get();
        }
        
        System.out.println("Chargement terminé: " + totalRows + " lignes importées");
    }
    
    /**
     * Traite un lot de lignes CSV
     * @param table Table cible
     * @param batch Lot de lignes à traiter
     * @param headers En-têtes des colonnes
     * @return Nombre de lignes traitées
     */
    private int processBatch(Table table, List<String> batch, List<String> headers) {
        int count = 0;
        
        for (String line : batch) {
            if (line.trim().isEmpty()) {
                continue;
            }
            
            String[] values = line.split(",", -1);
            
            // Vérifier que nous avons le bon nombre de valeurs
            if (values.length != headers.size()) {
                System.err.println("Erreur: nombre de valeurs incorrect dans la ligne: " + line);
                continue;
            }
            
            // Créer un map pour les valeurs de ligne (maintenant Object au lieu de String)
            Map<String, Object> rowData = new HashMap<>();
            for (int i = 0; i < headers.size(); i++) {
                rowData.put(headers.get(i), values[i]);
            }
            
            try {
                table.addRow(rowData);
                count++;
            } catch (Exception e) {
                System.err.println("Erreur lors de l'ajout de la ligne: " + e.getMessage());
            }
        }
        
        return count;
    }
    
    /**
     * Charge des données à partir d'une source quelconque en utilisant un fournisseur de données
     * @param table Table cible
     * @param dataProvider Fournisseur de données
     * @throws ExecutionException En cas d'erreur d'exécution
     * @throws InterruptedException En cas d'interruption
     */
    public void loadFromProvider(Table table, DataProvider dataProvider) throws ExecutionException, InterruptedException {
        // Obtenir le nombre total d'éléments à traiter
        long totalItems = dataProvider.estimateSize();
        System.out.println("Chargement de " + totalItems + " éléments avec " + threadCount + " threads");
        
        // Diviser les données en lots pour traitement parallèle
        List<Future<Integer>> futures = new ArrayList<>();
        
        int offset = 0;
        while (offset < totalItems) {
            int currentOffset = offset;
            int currentBatchSize = (int) Math.min(batchSize, totalItems - offset);
            
            Future<Integer> future = executorService.submit(() -> {
                List<Map<String, String>> stringBatch = dataProvider.getBatch(currentOffset, currentBatchSize);
                int count = 0;
                
                for (Map<String, String> stringRowData : stringBatch) {
                    try {
                        // Convertir Map<String, String> en Map<String, Object>
                        Map<String, Object> rowData = new HashMap<>();
                        for (Map.Entry<String, String> entry : stringRowData.entrySet()) {
                            rowData.put(entry.getKey(), entry.getValue());
                        }
                        
                        table.addRow(rowData);
                        count++;
                    } catch (Exception e) {
                        System.err.println("Erreur lors de l'ajout de la ligne: " + e.getMessage());
                    }
                }
                
                return count;
            });
            
            futures.add(future);
            offset += batchSize;
        }
        
        // Attendre la fin de tous les traitements
        int totalRows = 0;
        for (Future<Integer> future : futures) {
            totalRows += future.get();
        }
        
        System.out.println("Chargement terminé: " + totalRows + " lignes importées");
    }
    
    /**
     * Ferme le pool de threads
     */
    public void shutdown() {
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(30, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
    
    /**
     * Interface pour les fournisseurs de données génériques
     */
    public interface DataProvider {
        /**
         * Estime le nombre total d'éléments disponibles
         * @return Estimation du nombre d'éléments
         */
        long estimateSize();
        
        /**
         * Récupère un lot de données
         * @param offset Position de départ
         * @param limit Nombre maximum d'éléments à récupérer
         * @return Liste des données sous forme de maps
         */
        List<Map<String, String>> getBatch(int offset, int limit);
    }
} 
 