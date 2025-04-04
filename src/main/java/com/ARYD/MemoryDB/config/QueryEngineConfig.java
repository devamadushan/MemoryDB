package com.ARYD.MemoryDB.config;

import com.ARYD.MemoryDB.query.QueryEngine;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class QueryEngineConfig {

    @Bean
    public QueryEngine queryEngine() {
        return new QueryEngine();
    }
} 
 