#!/bin/bash

# Set the project directory
PROJECT_DIR="/Users/nghilethanh/Project/UPWORK-Web-Scraping-for-Running-Race-Results-Data/project"

# Create logs directory if it doesn't exist
mkdir -p $PROJECT_DIR/logs

# Create data directory with today's date
TODAY=$(date +%Y%m%d)
DATA_DIR="$PROJECT_DIR/data_$TODAY"
mkdir -p $DATA_DIR

# Function to run spider for a specific event and year range
run_event() {
    local event_name=$1
    local start_year=$2
    local end_year=$3
    
    echo "Processing $event_name from $start_year to $end_year"
    for year in $(seq $start_year $end_year); do
        echo "Processing $event_name $year"
        cd $PROJECT_DIR
        scrapy crawl ifinish -a event_name="$event_name" -a year="$year" -o "$DATA_DIR/${year}_${event_name// /_}.json"
    done
}

# Function to run all events
run_all_events() {
    run_event "Ladakh Marathon" 2013 2019
    run_event "Hyderabad Marathon" 2016 2021
    run_event "New Delhi Marathon" 2018 2019
    run_event "SKF Goa Marathon" 2021 2023
    run_event "Standard Chartered Mumbai Marathon" 2016 2017
    run_event "Airtel Delhi Half Marathon" 2015 2018
    run_event "Shriram Properties Bengaluru Marathon" 2015 2018
    
    # New events added
    run_event "Tuffman Half Marathon Delhi" 2025 2025
    run_event "Tuffman Gurugram Half Marathon" 2023 2025
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
            run_event "Ladakh Marathon" 2013 2019
            ;;
        "hyderabad")
            run_event "Hyderabad Marathon" 2016 2021
            ;;
        "delhi")
            run_event "New Delhi Marathon" 2018 2019
            ;;
        "goa")
            run_event "SKF" 2021 2023
            ;;
        "scmb")
            run_event "Standard Chartered Mumbai Marathon" 2016 2017
            ;;
        "spbm")
            run_event "Shriram Properties Bengaluru Marathon" 2015 2018
            ;;
        "adhm")
            run_event "Airtel Delhi Half Marathon" 2015 2018
            ;;
        "tuffman_delhi")
            run_event "Tuffman Half Marathon Delhi" 2025 2025
            ;;
        "tuffman_gurugram")
            run_event "Tuffman Gurugram Half Marathon" 2023 2025
            ;;
        "nmdc_hyderabad")
            run_event "NMDC HYDERABAD MARATHON 2025" 2025 2025
            ;;
        *)
            echo "Invalid event name. Use: ladakh, hyderabad, delhi, goa, scmb, spbm, adhm, tuffman_delhi, or tuffman_gurugram"
            exit 1
            ;;
    esac
fi

echo "Processing complete. Results saved to $DATA_DIR/" 