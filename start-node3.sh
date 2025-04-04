#!/bin/bash
echo "Démarrage du nœud 3..."
java -jar target/MemoryDB-0.0.1-SNAPSHOT.jar --config=config/node3.properties --server.port=8082 