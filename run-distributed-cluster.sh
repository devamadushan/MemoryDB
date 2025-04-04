#!/bin/bash

# Run a 3-node distributed MemoryDB cluster for testing
echo "Starting 3-node distributed MemoryDB cluster..."

# Create logs directory if it doesn't exist
mkdir -p logs

# Stop previous instances if running
echo "Stopping any previous instances..."
pkill -f "java.*-Dserver.port=808[0-2]" || true
sleep 2

# Start the first node (master) on port 8080
echo "Starting node 1 (master) on port 8080..."
java -Dserver.id=1 -Dserver.port=8080 -jar target/MemoryDB.jar > logs/node1.log 2>&1 &
NODE1_PID=$!
echo "Node 1 started with PID $NODE1_PID"

# Wait for node 1 to start
echo "Waiting for node 1 to start..."
sleep 10

# Start the second node on port 8081
echo "Starting node 2 on port 8081..."
java -Dserver.id=2 -Dserver.port=8081 -jar target/MemoryDB.jar > logs/node2.log 2>&1 &
NODE2_PID=$!
echo "Node 2 started with PID $NODE2_PID"

# Start the third node on port 8082
echo "Starting node 3 on port 8082..."
java -Dserver.id=3 -Dserver.port=8082 -jar target/MemoryDB.jar > logs/node3.log 2>&1 &
NODE3_PID=$!
echo "Node 3 started with PID $NODE3_PID"

# Wait for all nodes to start
echo "Waiting for all nodes to start..."
sleep 10

# Initialize the cluster
echo "Initializing the cluster..."
curl -X POST http://localhost:8080/api/cluster/init \
     -H "Content-Type: application/json" \
     -d '{"nodeName":"node1","nodeAddress":"localhost","nodePort":8080}'

# Add node 2 to the cluster
echo "Adding node 2 to the cluster..."
curl -X POST http://localhost:8080/api/cluster/nodes \
     -H "Content-Type: application/json" \
     -d '{"nodeName":"node2","nodeAddress":"localhost","nodePort":8081}'

# Add node 3 to the cluster
echo "Adding node 3 to the cluster..."
curl -X POST http://localhost:8080/api/cluster/nodes \
     -H "Content-Type: application/json" \
     -d '{"nodeName":"node3","nodeAddress":"localhost","nodePort":8082}'

# Check cluster status
echo "Checking cluster status..."
curl -s http://localhost:8080/api/cluster/nodes | jq

echo "Distributed MemoryDB cluster is now running!"
echo "Access the API at: http://localhost:8080/api/"
echo ""
echo "To stop the cluster, run: pkill -f \"java.*-Dserver.port=808[0-2]\""
echo "To view logs: tail -f logs/node*.log"
echo ""
echo "PIDs for reference:"
echo "Node 1: $NODE1_PID"
echo "Node 2: $NODE2_PID"
echo "Node 3: $NODE3_PID" 