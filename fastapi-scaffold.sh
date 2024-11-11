#!/bin/bash

# Create main project directory structure
mkdir -p app/{routers,crud,schemas,models,external_services,utils}

# Create __init__.py files in all directories
touch app/__init__.py
touch app/main.py
touch app/dependencies.py

# Create __init__.py files in subdirectories
for dir in routers crud schemas models external_services utils; do
    touch app/$dir/__init__.py
done

# Create tests directory and files
mkdir -p tests
touch tests/__init__.py
touch tests/test_main.py

# Create root level files
touch requirements.txt README.md

# Print success message with tree structure
echo "FastAPI project structure created successfully!"

# Check if 'tree' command is available
if command -v tree &> /dev/null; then
    tree
else
    echo "Project structure created. Install 'tree' command to view the directory structure."
    echo "Directory structure:"
    find . -type d -o -type f | sed -e "s/[^-][^\/]*\// |/g" -e "s/|\([^ ]\)/|-\1/"
fi
