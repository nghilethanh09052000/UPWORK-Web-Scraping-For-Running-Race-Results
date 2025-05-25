#!/bin/bash

# Set the project directory
PROJECT_DIR="/Users/nghilethanh/Project/UPWORK-Web-Scraping-for-Running-Race-Results-Data/project"

# Create logs directory if it doesn't exist
mkdir -p $PROJECT_DIR/logs

# Create data directory with today's date
TODAY=$(date +%Y%m%d)
DATA_DIR="$PROJECT_DIR/data_$TODAY"
mkdir -p $DATA_DIR

# Function to run spider for a specific event and year
run_event() {
    local event_name=$1
    local year=$2
    
    echo "Processing $event_name $year"
    cd $PROJECT_DIR
    scrapy crawl sportstimingsolutions -a event_name="$event_name" -a year="$year" -o "$DATA_DIR/${year}_${event_name// /_}.json"
}

# Function to run all events
run_all_events() {
    # Ladakh Marathon (2022, 2023)
    run_event "Ladakh Marathon" 2022
    run_event "Ladakh Marathon" 2023

    # Mumbai Marathon (2020)
    run_event "Mumbai Marathon" 2020

    # TCS World 10K Bengaluru (2025)
    run_event "TCS World 10K Bengaluru" 2025

    # Satara Hill Half Marathon (2019, 2021, 2022, 2023)
    run_event "Satara Hill Half Marathon" 2019
    run_event "Satara Hill Half Marathon" 2021
    run_event "Satara Hill Half Marathon" 2022
    run_event "Satara Hill Half Marathon" 2023
}

# Main script
cd $PROJECT_DIR

# Check if an event name is provided as argument
if [ $# -eq 0 ]; then
    # No arguments, run all events
    run_all_events
else
    # Run specific event based on argument
    case "$1" in
        "ladakh")
            run_event "Ladakh Marathon" 2022
            run_event "Ladakh Marathon" 2023
            ;;
        "mumbai")
            run_event "Mumbai Marathon" 2020
            ;;
        "tcs")
            run_event "TCS World 10K Bengaluru" 2025
            ;;
        "satara")
            run_event "Satara Hill Half Marathon" 2019
            run_event "Satara Hill Half Marathon" 2021
            run_event "Satara Hill Half Marathon" 2022
            run_event "Satara Hill Half Marathon" 2023
            ;;
        *)
            echo "Invalid event name. Use: ladakh, mumbai, tcs, or satara"
            exit 1
            ;;
    esac
fi

echo "Processing complete. Results saved to $DATA_DIR/" 