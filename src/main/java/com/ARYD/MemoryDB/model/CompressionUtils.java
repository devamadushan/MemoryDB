package com.ARYD.MemoryDB.model;


import com.github.luben.zstd.Zstd;

public class CompressionUtils {

    // Compression avec ZSTD
    public static byte[] compressZstd(byte[] data) {
        return Zstd.compress(data);
    }

    // Décompression avec ZSTD
    public static byte[] decompressZstd(byte[] compressedData) {
        return Zstd.decompress(compressedData, compressedData.length * 10); // Estimation de la taille initiale
    }
}
