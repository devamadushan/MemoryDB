package com.ARYD.MemoryDB.config;

import com.ARYD.MemoryDB.index.IndexFactory;
import com.ARYD.MemoryDB.storage.Database;
import com.ARYD.network.ClusterConfig;
import com.ARYD.network.ClusterManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

/**
 * Configuration des beans Spring
 */
@Configuration
public class BeanConfig {

    /**
     * Create a ClusterConfig instance first
     */
    @Bean
    @Primary
    public ClusterConfig clusterConfig() {
        return new ClusterConfig();
    }
    
    /**
     * Create ClusterManager with the ClusterConfig dependency
     */
    @Bean
    @Primary
    public ClusterManager clusterManager(ClusterConfig clusterConfig) {
        return new ClusterManager(clusterConfig);
    }
    
    /**
     * Crée une instance de IndexFactory comme bean Spring
     */
    @Bean
    public IndexFactory indexFactory() {
        return new IndexFactory();
    }
    
    /**
     * Crée une instance de Database comme bean Spring
     */
    @Bean
    public Database database(IndexFactory indexFactory) {
        return new Database(indexFactory);
    }
} 