#!/bin/bash

# Requires numpy to be installed globally

# Step 0: Remove old files
echo "Step 0: Remove old files"
rm -f ./data.json{,.offset}
rm -f data-1k.json

# Generate new data
echo "Step 1: Generate new data"
python3 generate_json.py

# Support --test-mode
echo "Step 3: Support --test-mode"
head -n 1000 data.json > data-1k.json

echo "Feel free to use!"
