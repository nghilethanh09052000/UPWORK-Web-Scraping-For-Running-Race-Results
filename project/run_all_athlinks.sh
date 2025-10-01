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

# Function to run spider for a specific master event ID with proxy range
run_master_event() {
    local master_id=$1
    local proxy_start=$2
    local proxy_end=$3
    
    echo "Processing master event ID: $master_id with proxies $proxy_start-$proxy_end"
    cd $PROJECT_DIR
    scrapy crawl athlinks_master \
        -a master_id="$master_id" \
        -s PROXY_START="$proxy_start" \
        -s PROXY_END="$proxy_end" \
        -o "$DATA_DIR/${master_id}.json"
    
    if [ $? -ne 0 ]; then
        echo "❌ Error processing master event ID: $master_id"
    else
        echo "✅ Successfully processed master event ID: $master_id"
    fi
}

# Function to run all master events
run_all_master_events() {
    # Run each master event in parallel with different proxy ranges
    run_master_event "34933" 0 50 &
    run_master_event "34538" 50 100 &
    run_master_event "103050" 0 50 &
    run_master_event "34504" 150 250 &
    run_master_event "34812" 600 650 &
    run_master_event "34792" 850 1000 &
    
    # New events added
    run_master_event "34990" 100 150 &  # BARMER Alsterlauf Hamburg
    run_master_event "34440" 200 300 &  # Standard Chartered Hong Kong Marathon
    run_master_event "4476" 300 400 &   # Houston Marathon
    run_master_event "3281" 400 500 &   # Marine Corps Marathon
    run_master_event "3241" 500 600 &   # CIM
    
    # Additional new events
    run_master_event "34524" 650 700 &  # Munich Half
    run_master_event "34908" 700 750 &  # Generali Genève Marathon
    run_master_event "187582" 750 800 & # GENERALI Berlin Half Marathon
    run_master_event "131958" 800 850 & # HASPA Marathon Hamburg
    run_master_event "34631" 900 950 &  # Hella Hamburg Halbmarathon
    run_master_event "34455" 900 950 &  # Sydney Marathon Halbmarathon
}

# Main script
cd $PROJECT_DIR

# Check if a master event ID is provided as argument
if [ $# -eq 0 ]; then
    # No arguments, run all master events
    run_all_master_events
else
    # Run specific master event based on argument
    case "$1" in
        "34933")
            run_master_event "34933" 0 50
            ;;

        "34538")
            run_master_event "34538" 50 100
            ;;

        "34504")
            run_master_event "34504" 150 250
            ;;

        "34812")
            run_master_event "34812" 600 650
            ;;

        "34792")
            run_master_event "34792" 850 1000
            ;;
            
        "103050")
            run_master_event "103050" 0 50
            ;;
        "34990")
            run_master_event "34990" 100 150
            ;;
        "34440")
            run_master_event "34440" 200 300
            ;;
        "4476")
            run_master_event "4476" 300 400
            ;;
        "3281")
            run_master_event "3281" 400 500
            ;;
        "3241")
            run_master_event "3241" 500 600
            ;;
        "34524")
            run_master_event "34524" 650 700
            ;;
        "34908")
            run_master_event "34908" 700 750
            ;;
        "187582")
            run_master_event "187582" 750 800
            ;;
        "131958")
            run_master_event "131958" 800 850
            ;;
        "34631")
            run_master_event "34631" 900 950
            ;;
        "34455")
            run_master_event "34455" 900 950
            ;;
        *)
            echo "Invalid master event ID. Use: 34933, 34538, 103050, 34504, 34812, 34792, 34990, 34440, 4476, 3281, 3241, 34524, 34908, 187582, 131958, or 34631"
            exit 1
            ;;
    esac
fi

echo "Processing complete. Results saved to $DATA_DIR/"