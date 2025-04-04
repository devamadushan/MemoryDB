#!/bin/bash

# Test script for the distributed MemoryDB cluster
# Make sure to run run-distributed-cluster.sh first

# Path to a test Parquet file - update this to match your environment
PARQUET_FILE="/path/to/your/sample.parquet"
echo "Using Parquet file: $PARQUET_FILE"
echo ""

# Check if the cluster is running
echo "Checking cluster status..."
CLUSTER_STATUS=$(curl -s http://localhost:8080/api/cluster/health)
if [[ $CLUSTER_STATUS == *"error"* ]]; then
    echo "Error: Cluster is not running. Please run run-distributed-cluster.sh first."
    exit 1
fi
echo "Cluster is running."
echo ""

# Inspect the Parquet file structure
echo "Inspecting Parquet file structure..."
curl -s "http://localhost:8080/api/tables/inspect-parquet?filePath=$PARQUET_FILE" | jq
echo ""

# Create a distributed table from the Parquet file schema
echo "Creating a distributed table from Parquet schema..."
curl -s -X POST http://localhost:8080/api/tables/create-table-schema \
     -H "Content-Type: application/json" \
     -d "{\"tableName\":\"distributed_test\",\"sourceFilePath\":\"$PARQUET_FILE\",\"distributed\":true}" | jq
echo ""

# Verify that the table was created on all nodes
echo "Verifying table creation on all nodes..."
curl -s http://localhost:8080/api/tables | jq
echo ""

# Load data with round-robin distribution
echo "Loading data with round-robin distribution..."
curl -s -X POST http://localhost:8080/api/tables/load-parquet-distributed \
     -H "Content-Type: application/json" \
     -d "{\"tableName\":\"distributed_test\",\"filePath\":\"$PARQUET_FILE\",\"maxRows\":1000,\"distributionStrategy\":\"ROUND_ROBIN\"}" | jq
echo ""

# Wait for loading to complete
echo "Waiting for data loading to complete (20 seconds)..."
sleep 20

# Check data distribution
echo "Checking data distribution across nodes..."
curl -s http://localhost:8080/api/tables/distributed_test/stats | jq
echo ""

# Execute a distributed query
echo "Executing a distributed query..."
curl -s -X POST http://localhost:8080/api/query \
     -H "Content-Type: text/plain" \
     -d "SELECT * FROM distributed_test LIMIT 10" | jq
echo ""

# Test more complex query
echo "Executing a more complex distributed query..."
curl -s -X POST http://localhost:8080/api/query \
     -H "Content-Type: text/plain" \
     -d "SELECT COUNT(*) as count FROM distributed_test" | jq
echo ""

echo "Test completed successfully!" 