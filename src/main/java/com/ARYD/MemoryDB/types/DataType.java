package com.ARYD.MemoryDB.types;

import java.io.Serializable;
import java.nio.ByteBuffer;

/**
 * Classe abstraite servant de base pour tous les types de données supportés
 * dans la base de données en mémoire.
 */
public abstract class DataType implements Serializable {
    private static final long serialVersionUID = 1L;
    
    // Taille en octets du type dans la représentation binaire
    protected int sizeInBytes;
    
    public DataType() {
        // Constructeur par défaut
    }
    
    /**
     * Retourne la classe Java associée à ce type
     */
    public abstract Class<?> getAssociatedClassType();
    
    /**
     * Convertit une chaîne en valeur typée et l'écrit dans le buffer fourni
     * @param input Chaîne à convertir
     * @param outputBuffer Buffer dans lequel écrire la valeur
     * @return La valeur convertie
     */
    public abstract Object parseAndWriteToBuffer(String input, ByteBuffer outputBuffer) throws IllegalArgumentException;
    
    /**
     * Lit la valeur depuis un tableau d'octets
     * @param bytes Tableau d'octets contenant la valeur
     * @return La valeur lue avec son type Java approprié
     */
    public abstract Object readTrueValue(byte[] bytes);
    
    /**
     * Lit la valeur utilisée pour l'indexation
     * @param bytes Tableau d'octets contenant la valeur
     * @return La valeur à utiliser pour l'indexation
     */
    public abstract Object readIndexValue(byte[] bytes);
    
    /**
     * Vérifie si l'opérateur est compatible avec ce type
     * @param op Opérateur à vérifier
     * @return true si l'opérateur est compatible, false sinon
     */
    public abstract boolean isOperatorCompatible(Operator op);
    
    /**
     * Vérifie si une chaîne peut être convertie en ce type
     * @param input Chaîne à vérifier
     * @return true si la chaîne peut être convertie, false sinon
     */
    public abstract boolean inputCanBeParsed(String input);
    
    /**
     * Retourne la valeur par défaut pour ce type
     * @return La valeur par défaut
     */
    public abstract Object getDefaultValue();
    
    /**
     * Retourne la taille en octets de ce type
     * @return Taille en octets
     */
    public int getSizeInBytes() {
        return sizeInBytes;
    }
    
    /**
     * Indique si la taille est fixe ou variable
     * @return true si la taille est fixe, false sinon
     */
    public abstract boolean isFixedSize();
} 
 