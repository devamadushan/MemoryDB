package com.ARYD.MemoryDB.config;

import com.ARYD.MemoryDB.util.ParquetInspector;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Configuration pour les utilitaires et services liés à Parquet.
 */
@Configuration
public class ParquetConfig {

    /**
     * Crée un bean ParquetInspector pour inspecter les fichiers Parquet.
     * 
     * @return L'instance du ParquetInspector
     */
    @Bean
    public ParquetInspector parquetInspector() {
        return new ParquetInspector();
    }
} 
 