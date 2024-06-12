#!/bin/bash
echo "Setting up environment..."
echo "Running tests..."
python -m unittest test_pipeline.py
if [ $? -eq 0 ]; then
    echo "Tests passed successfully"
else
    echo "Tests failed"
fi
