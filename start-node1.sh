#!/bin/bash
echo "Démarrage du nœud 1 (Leader)..."
java -jar target/MemoryDB-0.0.1-SNAPSHOT.jar --config=config/node1.properties --server.port=8080 