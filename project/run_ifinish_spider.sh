#!/bin/bash

# Set the project directory
PROJECT_DIR="/Users/nghilethanh/Project/UPWORK-Web-Scraping-for-Running-Race-Results-Data/project"

# Create logs directory if it doesn't exist
mkdir -p "$PROJECT_DIR/logs"

# Run the spider with event name and year
cd "$PROJECT_DIR" && \
scrapy crawl ifinish \
    -a event_name="Hyderabad Marathon 2024" \
    -a year="2024" \
    -o "data/ifinish_results.json"

echo "Spider completed!" 