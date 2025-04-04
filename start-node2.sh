#!/bin/bash
echo "Démarrage du nœud 2..."
java -jar target/MemoryDB-0.0.1-SNAPSHOT.jar --config=config/node2.properties --server.port=8081 