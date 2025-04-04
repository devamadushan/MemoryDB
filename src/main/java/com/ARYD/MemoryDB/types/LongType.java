package com.ARYD.MemoryDB.types;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Implémentation du type Long pour le stockage et la manipulation efficace
 * des entiers 64 bits
 */
public class LongType extends DataType {
    private static final long serialVersionUID = 1L;
    
    public LongType() {
        super();
        this.sizeInBytes = Long.BYTES;
    }
    
    @Override
    public Class<?> getAssociatedClassType() {
        return Long.class;
    }
    
    @Override
    public Object parseAndWriteToBuffer(String input, ByteBuffer outputBuffer) throws IllegalArgumentException {
        try {
            Long valueAsLong = Long.parseLong(input);
            outputBuffer.putLong(valueAsLong);
            return valueAsLong;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Impossible de convertir '" + input + "' en Long: " + e.getMessage());
        }
    }
    
    @Override
    public Long readTrueValue(byte[] bytes) {
        ByteBuffer wrapped = ByteBuffer.wrap(bytes);
        return wrapped.getLong();
    }
    
    @Override
    public Long readIndexValue(byte[] bytes) {
        ByteBuffer wrapped = ByteBuffer.wrap(bytes);
        return wrapped.getLong();
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
            Operator.NOT_EQUALS,
            Operator.IN
        ).contains(op);
    }
    
    @Override
    public boolean inputCanBeParsed(String input) {
        try {
            Long.parseLong(input);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }
    
    @Override
    public Object getDefaultValue() {
        return 0L;
    }
    
    @Override
    public boolean isFixedSize() {
        return true;
    }
} 
 