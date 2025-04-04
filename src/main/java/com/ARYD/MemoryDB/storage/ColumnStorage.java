package com.ARYD.MemoryDB.storage;

import com.ARYD.MemoryDB.types.DataType;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Stockage colonnaire optimisé pour les données.
 * Utilise des chunks pour améliorer l'efficacité du stockage et des opérations.
 */
public class ColumnStorage {
    
    private static final int DEFAULT_CHUNK_SIZE = 1024; // Taille par défaut des chunks (en nombre d'éléments)
    private static final int DEFAULT_BUFFER_SIZE = 4096; // Taille par défaut des buffers (en octets)
    
    private final String name;              // Nom de la colonne
    private final DataType dataType;        // Type de données
    private final int chunkSize;            // Taille des chunks
    
    private final List<ByteBuffer> chunks;  // Liste des chunks de données
    private ByteBuffer currentChunk;        // Chunk courant pour l'écriture
    private final AtomicInteger rowCount;   // Nombre total de lignes
    
    /**
     * Constructeur avec taille de chunk par défaut
     */
    public ColumnStorage(String name, DataType dataType) {
        this(name, dataType, DEFAULT_CHUNK_SIZE);
    }
    
    /**
     * Constructeur complet
     */
    public ColumnStorage(String name, DataType dataType, int chunkSize) {
        this.name = name;
        this.dataType = dataType;
        this.chunkSize = chunkSize;
        this.chunks = new ArrayList<>();
        this.rowCount = new AtomicInteger(0);
        
        // Créer le premier chunk
        createNewChunk();
    }
    
    /**
     * Ajoute une valeur à la colonne
     * @param value Valeur à ajouter (sous forme de chaîne)
     * @return Index de la ligne ajoutée
     */
    public int addValue(String value) {
        // Vérifier si le chunk courant est plein
        if (!currentChunk.hasRemaining()) {
            createNewChunk();
        }
        
        // Parser et écrire la valeur dans le buffer
        try {
            dataType.parseAndWriteToBuffer(value, currentChunk);
        } catch (IllegalArgumentException e) {
            // En cas d'erreur, utiliser la valeur par défaut
            currentChunk.position(currentChunk.position() - dataType.getSizeInBytes());
            Object defaultValue = dataType.getDefaultValue();
            dataType.parseAndWriteToBuffer(defaultValue.toString(), currentChunk);
        }
        
        // Incrémenter et retourner l'index
        return rowCount.getAndIncrement();
    }
    
    /**
     * Ajoute une valeur à la colonne
     * @param value Valeur à ajouter (objet)
     * @return Index de la ligne ajoutée
     */
    public int addValue(Object value) {
        if (value == null) {
            // Utiliser la valeur par défaut si null
            return addValue(dataType.getDefaultValue().toString());
        } else {
            // Convertir l'objet en chaîne
            return addValue(value.toString());
        }
    }
    
    /**
     * Lit une valeur à l'index spécifié
     * @param index Index de la valeur à lire
     * @return Valeur lue
     */
    public Object getValue(int index) {
        if (index < 0 || index >= rowCount.get()) {
            throw new IndexOutOfBoundsException("Index hors limites: " + index);
        }
        
        // Calculer le chunk et l'offset dans le chunk
        int chunkIndex = index / chunkSize;
        int offsetInChunk = index % chunkSize;
        
        // Récupérer le chunk
        ByteBuffer chunk = chunks.get(chunkIndex);
        
        // Calculer la position dans le chunk
        int position = offsetInChunk * dataType.getSizeInBytes();
        
        // Lire les octets
        byte[] bytes = new byte[dataType.getSizeInBytes()];
        ByteBuffer duplicate = chunk.duplicate();
        duplicate.position(position);
        duplicate.get(bytes);
        
        // Convertir en valeur
        return dataType.readTrueValue(bytes);
    }
    
    /**
     * Retourne le nombre de valeurs dans la colonne
     */
    public int size() {
        return rowCount.get();
    }
    
    /**
     * Retourne le nom de la colonne
     */
    public String getName() {
        return name;
    }
    
    /**
     * Retourne le type de données de la colonne
     */
    public DataType getDataType() {
        return dataType;
    }
    
    /**
     * Crée un nouveau chunk pour le stockage
     */
    private void createNewChunk() {
        int bufferSize;
        
        // Si le type a une taille fixe, on peut calculer la taille exacte du buffer
        if (dataType.isFixedSize()) {
            bufferSize = chunkSize * dataType.getSizeInBytes();
        } else {
            // Sinon, on utilise une taille par défaut
            bufferSize = DEFAULT_BUFFER_SIZE;
        }
        
        currentChunk = ByteBuffer.allocate(bufferSize);
        chunks.add(currentChunk);
    }
    
    /**
     * Applique une fonction à chaque valeur de la colonne
     * @param processor Fonction à appliquer
     */
    public void forEach(ValueProcessor processor) {
        int size = rowCount.get();
        for (int i = 0; i < size; i++) {
            processor.process(i, getValue(i));
        }
    }
    
    /**
     * Interface fonctionnelle pour le traitement des valeurs
     */
    @FunctionalInterface
    public interface ValueProcessor {
        void process(int index, Object value);
    }
} 
 