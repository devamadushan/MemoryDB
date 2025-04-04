#!/bin/bash
echo "Démarrage du cluster MemoryDB à 3 nœuds..."

# Démarrer le nœud 1 (leader) en premier plan avec un log
echo "Démarrage du nœud 1 (Leader)..."
java -jar target/MemoryDB-0.0.1-SNAPSHOT.jar --config=config/node1.properties --server.port=8080 > logs-node1.log 2>&1 &
PID1=$!
echo "Nœud 1 démarré avec PID: $PID1"

# Attendre quelques secondes pour que le leader soit prêt
sleep 5

# Démarrer le nœud 2
echo "Démarrage du nœud 2..."
java -jar target/MemoryDB-0.0.1-SNAPSHOT.jar --config=config/node2.properties --server.port=8081 > logs-node2.log 2>&1 &
PID2=$!
echo "Nœud 2 démarré avec PID: $PID2"

# Démarrer le nœud 3
echo "Démarrage du nœud 3..."
java -jar target/MemoryDB-0.0.1-SNAPSHOT.jar --config=config/node3.properties --server.port=8082 > logs-node3.log 2>&1 &
PID3=$!
echo "Nœud 3 démarré avec PID: $PID3"

echo "Tous les nœuds sont démarrés. Pour voir les logs, utilisez:"
echo "tail -f logs-node1.log"
echo "tail -f logs-node2.log"
echo "tail -f logs-node3.log"
echo ""
echo "Pour arrêter le cluster:"
echo "kill $PID1 $PID2 $PID3"

# Enregistrer les PIDs pour pouvoir arrêter facilement
echo "$PID1 $PID2 $PID3" > cluster-pids.txt
echo "PIDs sauvegardés dans cluster-pids.txt" 