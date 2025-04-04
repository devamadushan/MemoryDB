package com.ARYD.MemoryDB.types;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Implémentation du type String avec compression par dictionnaire
 * pour une utilisation efficace de la mémoire.
 */
public class StringType extends DataType {
    private static final long serialVersionUID = 1L;
    
    // Par défaut, on stocke l'index (4 octets) plutôt que la chaîne entière
    private static final int DEFAULT_SIZE = Integer.BYTES;
    
    // Dictionnaires partagés pour économiser de la mémoire
    private static final Map<String, Integer> stringToIdMap = new ConcurrentHashMap<>();
    private static final Map<Integer, String> idToStringMap = new ConcurrentHashMap<>();
    private static final AtomicInteger nextId = new AtomicInteger(0);
    
    // Taille maximale pour la version non compressée
    private final int maxUncompressedSize;
    private final boolean useCompression;
    
    /**
     * Crée un StringType avec compression par dictionnaire
     */
    public StringType() {
        this(true, 0);
    }
    
    /**
     * Crée un StringType avec option de compression
     * @param useCompression Utiliser la compression par dictionnaire
     * @param maxSize Taille maximale pour les chaînes non compressées (0 = illimité)
     */
    public StringType(boolean useCompression, int maxSize) {
        super();
        this.useCompression = useCompression;
        this.maxUncompressedSize = maxSize;
        
        // Si on utilise la compression, on stocke juste l'ID (4 octets)
        if (useCompression) {
            this.sizeInBytes = DEFAULT_SIZE;
        } else if (maxSize > 0) {
            // Sinon, si on a une taille fixe, on utilise cette taille + 2 octets pour la longueur
            this.sizeInBytes = maxSize + 2;
        } else {
            // Pour les chaînes de taille variable, on ne peut pas prédire la taille exacte
            this.sizeInBytes = -1;
        }
    }
    
    @Override
    public Class<?> getAssociatedClassType() {
        return String.class;
    }
    
    @Override
    public Object parseAndWriteToBuffer(String input, ByteBuffer outputBuffer) throws IllegalArgumentException {
        if (input == null) {
            input = "";
        }
        
        if (useCompression) {
            // Avec compression: on stocke un ID qui pointe vers la chaîne dans le dictionnaire
            Integer id = stringToIdMap.computeIfAbsent(input, k -> nextId.getAndIncrement());
            idToStringMap.putIfAbsent(id, input);
            outputBuffer.putInt(id);
            return input;
        } else {
            // Sans compression: on stocke la chaîne directement
            byte[] bytes = input.getBytes(StandardCharsets.UTF_8);
            
            // Si on a une taille fixe, on tronque ou on remplit de zéros
            if (maxUncompressedSize > 0) {
                if (bytes.length > maxUncompressedSize) {
                    // Tronquer
                    outputBuffer.putShort((short) maxUncompressedSize);
                    outputBuffer.put(bytes, 0, maxUncompressedSize);
                } else {
                    // Remplir
                    outputBuffer.putShort((short) bytes.length);
                    outputBuffer.put(bytes);
                    // Remplir de zéros le reste
                    for (int i = bytes.length; i < maxUncompressedSize; i++) {
                        outputBuffer.put((byte) 0);
                    }
                }
            } else {
                // Taille variable: on stocke la longueur puis les octets
                outputBuffer.putShort((short) bytes.length);
                outputBuffer.put(bytes);
            }
            
            return input;
        }
    }
    
    @Override
    public String readTrueValue(byte[] bytes) {
        ByteBuffer wrapped = ByteBuffer.wrap(bytes);
        
        if (useCompression) {
            // Avec compression: on récupère la chaîne à partir de l'ID
            int id = wrapped.getInt();
            return idToStringMap.getOrDefault(id, "");
        } else {
            // Sans compression: on lit la longueur puis les octets
            short length = wrapped.getShort();
            byte[] stringBytes = new byte[length];
            wrapped.get(stringBytes);
            return new String(stringBytes, StandardCharsets.UTF_8);
        }
    }
    
    @Override
    public String readIndexValue(byte[] bytes) {
        // Pour l'indexation, on utilise la même valeur
        return readTrueValue(bytes);
    }
    
    @Override
    public boolean isOperatorCompatible(Operator op) {
        return Arrays.asList(
            Operator.EQUALS,
            Operator.NOT_EQUALS,
            Operator.LIKE,
            Operator.CONTAINS,
            Operator.STARTS_WITH,
            Operator.ENDS_WITH,
            Operator.IN
        ).contains(op);
    }
    
    @Override
    public boolean inputCanBeParsed(String input) {
        // Toute chaîne est valide
        return true;
    }
    
    @Override
    public Object getDefaultValue() {
        return "";
    }
    
    @Override
    public boolean isFixedSize() {
        // Avec compression ou taille fixe, la taille est prévisible
        return useCompression || maxUncompressedSize > 0;
    }
    
    /**
     * Vide le dictionnaire partagé (utile pour les tests)
     */
    public static void clearDictionary() {
        stringToIdMap.clear();
        idToStringMap.clear();
        nextId.set(0);
    }
    
    /**
     * Retourne la taille du dictionnaire
     */
    public static int getDictionarySize() {
        return stringToIdMap.size();
    }
} 
 