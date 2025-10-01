#!/bin/bash

# Set the project directory
PROJECT_DIR="/Users/nghilethanh/Project/UPWORK-Web-Scraping-for-Running-Race-Results-Data/project"
#source .venv/bin/activate

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
    local proxy_start=$3
    local proxy_end=$4
    
    echo "Processing event: $event_name, year: $year with proxies $proxy_start-$proxy_end"
    cd $PROJECT_DIR
    scrapy crawl multisportaustralia \
        -a event_name="$event_name" \
        -a year="$year" \
        -s PROXY_START="$proxy_start" \
        -s PROXY_END="$proxy_end" \
        -o "$DATA_DIR/${event_name}_${year}.json"
    
    if [ $? -ne 0 ]; then
        echo "❌ Error processing event: $event_name, year: $year"
    else
        echo "✅ Successfully processed event: $event_name, year: $year"
    fi
}

# Function to run all events
run_all_events() {
    # Run each event in parallel with different proxy ranges
    run_event "sydney-marathon-2024" "2024" 0 500 &
    run_event "sydney-marathon-2023" "2023" 50 100 &
    run_event "blackmores-sydney-running-festival-2022" "2022" 250 750 &
    
    # Wait for all background processes to complete
    wait
}

# Main script
cd $PROJECT_DIR

# Check if arguments are provided
if [ $# -eq 0 ]; then
    # No arguments, run all events
    run_all_events
elif [ $# -eq 2 ]; then
    # Two arguments provided: event_name and year (use default proxy range)
    event_name=$1
    year=$2
    run_event "$event_name" "$year" 0 50
elif [ $# -eq 4 ]; then
    # Four arguments provided: event_name, year, proxy_start, proxy_end
    event_name=$1
    year=$2
    proxy_start=$3
    proxy_end=$4
    run_event "$event_name" "$year" "$proxy_start" "$proxy_end"
else
    # Invalid number of arguments
    echo "Usage: $0 [event_name year [proxy_start proxy_end]]"
    echo "Examples:"
    echo "  $0                                    # Run all events"
    echo "  $0 sydney-marathon-2024 2024         # Run specific event with default proxies"
    echo "  $0 sydney-marathon-2023 2023         # Run specific event with default proxies"
    echo "  $0 blackmores-sydney-running-festival-2022 2022  # Run specific event with default proxies"
    echo "  $0 sydney-marathon-2024 2024 0 50    # Run specific event with custom proxy range"
    exit 1
fi

echo "Processing complete. Results saved to $DATA_DIR/" 