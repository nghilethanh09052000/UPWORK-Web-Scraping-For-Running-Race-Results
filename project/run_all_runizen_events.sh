#!/bin/bash

# Set the project directory
PROJECT_DIR="/Users/nghilethanh/Project/UPWORK-Web-Scraping-for-Running-Race-Results-Data/project"
source .venv/bin/activate
# Create logs directory if it doesn't exist
mkdir -p $PROJECT_DIR/logs

# Create data directory with today's date
TODAY=$(date +%Y%m%d)
DATA_DIR="$PROJECT_DIR/data_$TODAY"
mkdir -p $DATA_DIR

# Function to run spider for a specific event and year with bib range
run_event() {
    local event_name=$1
    local year=$2
    local start_bib=$3
    local end_bib=$4
    
    echo "Processing $event_name $year (Bibs: $start_bib-$end_bib)"
    cd $PROJECT_DIR
    scrapy crawl runizen -a event_name="$event_name" -a year="$year" -a start_bib="$start_bib" -a end_bib="$end_bib" -o "$DATA_DIR/${year}_${event_name// /_}.json"
}

# Function to run all events
run_all_events() {
    # Vedanta Pink City Half (2022-2024)
    run_event "Vedanta Pink City Half" 2022 1 50000
    run_event "Vedanta Pink City Half" 2023 1 50000
    run_event "Vedanta Pink City Half" 2024 1 50000

    # Vedanta Zinc City Half (2024)
    run_event "Vedanta Zinc City Half" 2024 1 50000
    
    # New events added
    # Sonipat Half Marathon (2025)
    run_event "Sonipat Half Marathon" 2025 1 50000
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
        "pink")
            # Vedanta Pink City Half
            run_event "Vedanta Pink City Half" 2022 1 50000
            run_event "Vedanta Pink City Half" 2023 1 50000
            run_event "Vedanta Pink City Half" 2024 1 50000
            ;;
        "zinc")
            # Vedanta Zinc City Half
            run_event "Vedanta Zinc City Half" 2024 1 50000
            ;;
        "sonipat")
            # Sonipat Half Marathon
            run_event "Sonipat Half Marathon" 2025 1 50000
            ;;
        *)
            echo "Invalid event name. Use: pink, zinc, or sonipat"
            exit 1
            ;;
    esac
fi

echo "Processing complete. Results saved to $DATA_DIR/" 