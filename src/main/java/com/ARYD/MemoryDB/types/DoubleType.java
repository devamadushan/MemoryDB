package com.ARYD.MemoryDB.types;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Implémentation du type Double pour le stockage et la manipulation efficace
 * des nombres à virgule flottante double précision
 */
public class DoubleType extends DataType {
    private static final long serialVersionUID = 1L;
    
    public DoubleType() {
        super();
        this.sizeInBytes = Double.BYTES;
    }
    
    @Override
    public Class<?> getAssociatedClassType() {
        return Double.class;
    }
    
    @Override
    public Object parseAndWriteToBuffer(String input, ByteBuffer outputBuffer) throws IllegalArgumentException {
        try {
            Double valueAsDouble = Double.parseDouble(input);
            outputBuffer.putDouble(valueAsDouble);
            return valueAsDouble;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Impossible de convertir '" + input + "' en Double: " + e.getMessage());
        }
    }
    
    @Override
    public Double readTrueValue(byte[] bytes) {
        ByteBuffer wrapped = ByteBuffer.wrap(bytes);
        return wrapped.getDouble();
    }
    
    @Override
    public Double readIndexValue(byte[] bytes) {
        ByteBuffer wrapped = ByteBuffer.wrap(bytes);
        return wrapped.getDouble();
    }
    
    @Override
    public boolean isOperatorCompatible(Operator op) {
        return Arrays.asList(
            Operator.EQUALS,
            Operator.GREATER,
            Operator.LESS,
            Operator.GREATER_OR_EQUALS,
            Operator.LESS_OR_EQUALS,
            Operator.BETWEEN,
            Operator.NOT_EQUALS
        ).contains(op);
    }
    
    @Override
    public boolean inputCanBeParsed(String input) {
        try {
            Double.parseDouble(input);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }
    
    @Override
    public Object getDefaultValue() {
        return 0.0;
    }
    
    @Override
    public boolean isFixedSize() {
        return true;
    }
    
    /**
     * Méthode utilitaire pour comparer deux doubles avec une précision epsilon
     */
    public static boolean equals(double a, double b, double epsilon) {
        return Math.abs(a - b) < epsilon;
    }
    
    /**
     * Méthode utilitaire pour comparer deux doubles avec la précision par défaut
     */
    public static boolean equals(double a, double b) {
        return equals(a, b, 0.0000001);
    }
} 
 