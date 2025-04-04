#!/bin/bash
echo "Arrêt du cluster MemoryDB..."

if [ -f cluster-pids.txt ]; then
    PIDS=$(cat cluster-pids.txt)
    echo "Arrêt des nœuds avec PIDs: $PIDS"
    kill $PIDS 2>/dev/null
    echo "Cluster arrêté"
    rm cluster-pids.txt
else
    echo "Fichier cluster-pids.txt non trouvé. Tentative d'arrêt par recherche de processus Java..."
    ps aux | grep "MemoryDB.*config" | grep -v grep | awk '{print $2}' | xargs kill 2>/dev/null
    echo "Tentative d'arrêt terminée"
fi 