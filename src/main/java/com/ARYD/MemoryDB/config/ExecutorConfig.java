package com.ARYD.MemoryDB.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Configuration
public class ExecutorConfig {

    @Bean
    public ExecutorService executorService() {
        return Executors.newFixedThreadPool(
            // Utiliser le nombre de cœurs disponibles pour un traitement optimal
            Runtime.getRuntime().availableProcessors()
        );
    }
} 
 