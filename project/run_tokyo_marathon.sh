#!/bin/bash

# Tokyo Marathon Spider Runner Script
# This script runs the Tokyo Marathon spider to scrape race results

echo "Starting Tokyo Marathon Spider..."
echo "=================================="

# Change to the project directory
cd "$(dirname "$0")"

# Create output directory if it doesn't exist
mkdir -p data_tokyo_marathon

# Run the spider
scrapy crawl tokyo_marathon \
    -o data_tokyo_marathon/tokyo_marathon_results_$(date +%Y%m%d_%H%M%S).json \
    -L INFO

echo "Tokyo Marathon Spider completed!"
echo "Results saved in data_tokyo_marathon/ directory"
