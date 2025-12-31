#!/bin/bash

# Set the project directory
PROJECT_DIR="/Users/nghilethanh/Project/UPWORK-Web-Scraping-for-Running-Race-Results-Data/project"

# Create logs directory if it doesn't exist
mkdir -p $PROJECT_DIR/logs

# Create data directory with today's date
TODAY=$(date +%Y%m%d)
DATA_DIR="$PROJECT_DIR/data_$TODAY"
mkdir -p $DATA_DIR

# Function to run spider for a specific year
run_chicago_marathon() {
    local year=$1
    
    echo "Processing Chicago Marathon $year"
    cd $PROJECT_DIR
    scrapy crawl chicago_marathon \
        -a year="$year" \
        -o "$DATA_DIR/chicago_marathon_${year}.json"
    
    if [ $? -ne 0 ]; then
        echo "❌ Error processing Chicago Marathon $year"
    else
        echo "✅ Successfully processed Chicago Marathon $year"
    fi
}

# Main script
cd $PROJECT_DIR

# Check if a year is provided as argument
if [ $# -eq 0 ]; then
    # No arguments, run for 2025 by default
    run_chicago_marathon 2025
else
    # Run for specific year
    run_chicago_marathon "$1"
fi

echo "Processing complete. Results saved to $DATA_DIR/"

