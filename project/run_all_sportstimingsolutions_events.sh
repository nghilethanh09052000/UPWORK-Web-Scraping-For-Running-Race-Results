#!/bin/bash

# Set the project directory
PROJECT_DIR="/Users/nghilethanh/Project/UPWORK-Web-Scraping-for-Running-Race-Results-Data/project"

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
    scrapy crawl sportstimingsolutions -a event_name="$event_name" -a year="$year" -a start_bib="$start_bib" -a end_bib="$end_bib" -o "$DATA_DIR/${year}_${event_name// /_}.json"
}

# Function to run all events
run_all_events() {
    # Ladakh Marathon (2022, 2023)
    run_event "Ladakh Marathon" 2022 1000 10001
    run_event "Ladakh Marathon" 2023 1000 10001
    run_event "Ladakh Marathon" 2025 1000 10001

    # Mumbai Marathon (2020)
    run_event "Mumbai Marathon" 2020 1000 100001

    # TCS World 10K Bengaluru (2025)
    run_event "TCS World 10K Bengaluru" 2025 1 100000

    # Satara Hill Half Marathon (2019, 2021, 2022, 2023)
    # run_event "Satara Hill Half Marathon" 2019 1000 100001
    # run_event "Satara Hill Half Marathon" 2021 1000 100001
    # run_event "Satara Hill Half Marathon" 2022 1000 100001
    run_event "Satara Hill Half Marathon" 2023 1000 100001

    run_event "Vadodara Marathon" 2023 1 90000
    run_event "Vadodara Marathon" 2024 1 100001
    run_event "Vadodara Marathon" 2025 1 100001

    run_event "Bengaluru Marathon" 2019 1 100001
    run_event "Bengaluru Marathon" 2021 1 100001
    run_event "Bengaluru Marathon" 2023 1 100001
    run_event "Bengaluru Marathon" 2024 1 100001

    # New events added
    # Hiranandani Thane Half Marathon
    run_event "Hiranandani Thane Half Marathon 2025" 2025 1 30001

    # Apla Pune Marathon (2020 to current)
    run_event "Apla Pune Marathon" 2020 1 100001
    run_event "Apla Pune Marathon" 2021 1 100001
    run_event "Apla Pune Marathon" 2022 1 100001
    run_event "Apla Pune Marathon" 2023 1 100001
    run_event "Apla Pune Marathon" 2024 1 100001
    run_event "Apla Pune Marathon" 2025 1 100001

    # Rotary Valsad City Marathon (2023 to current)
    run_event "Rotary Valsad City Marathon" 2023 1 30001
    run_event "Rotary Valsad City Marathon" 2024 1 30001
    run_event "Rotary Valsad City Marathon" 2025 1 30001

    # Ekal Run Surat (2024, 2025)
    run_event "Ekal Run Surat" 2024 1 30001
    run_event "Ekal Run Surat" 2025 1 30001

    # Bajaj Allianz Half Marathon (2022 to current)
    run_event "Bajaj Allianz Pune Half Marathon 2022" 2022 1 100001
    run_event "Bajaj Allianz Pune Half Marathon 2023" 2023 1 100001
    run_event "Bajaj Allianz Pune Half Marathon 2024" 2024 1 100001
    run_event "Bajaj Allianz Pune Half Marathon 2025" 2025 1 100001

    # Disha Habitat Bengaluru Runners Jatre (2024, 2025)
    run_event "Disha Habitat Bengaluru Runners Jatre" 2024 1 100001
    run_event "Disha Habitat Bengaluru Runners Jatre" 2025 1 100001

    # New events added
    # Indian Navy Half Marathon (2020 to current)
    run_event "Indian Navy Half Marathon" 2020 1 100001
    run_event "Indian Navy Half Marathon" 2021 1 100001
    run_event "Indian Navy Half Marathon" 2022 1 100001
    run_event "Indian Navy Half Marathon" 2023 1 100001
    run_event "Indian Navy Half Marathon" 2024 1 100001
    run_event "Indian Navy Half Marathon" 2025 1 100001

    # Chandigarh Fast Marathon (2024, 2025)
    run_event "Chandigarh Fast Marathon" 2024 1 100001
    run_event "Chandigarh Fast Marathon" 2025 1 100001

    # Vasai Virar Municipal Corporation Marathon (2024, 2025)
    run_event "Vasai Virar Municipal Corporation Marathon" 2024 1 100001
    run_event "Vasai Virar Municipal Corporation Marathon" 2025 1 100001

    # Kashmir Marathon (2024)
    run_event "Kashmir Marathon" 2024 1 100001

    # SBI Patna Marathon Reloaded (2024)
    run_event "SBI Patna Marathon Reloaded" 2024 1 100001
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
            # Ladakh Marathon bib ranges
            # run_event "Ladakh Marathon" 2022 1000 10001
            # run_event "Ladakh Marathon" 2023 1000 10001
            run_event "Ladakh Marathon" 2025 1000 10001
            ;;
        "mumbai")
            # Mumbai Marathon bib ranges
            run_event "Mumbai Marathon" 2020 1000 100001
            ;;
        "tcs")
            # TCS World 10K Bengaluru bib ranges
            run_event "TCS World 10K Bengaluru" 2025 1 100000
            ;;
        "jbg")
            # TCS World 10K Bengaluru bib ranges
            run_event "JBG SHHM" 2023 1 100000
            ;;
        "vm")
            # Vadodara Marathon
            run_event "Vadodara Marathon" 2023 1 90000
            run_event "Vadodara Marathon" 2024 1 100001
            run_event "Vadodara Marathon" 2025 1 100001
            ;;
        "bm")
            # TCS World 10K Bengaluru bib ranges
            run_event "Bengaluru Marathon" 2019 1 100001
            run_event "Bengaluru Marathon" 2021 1 100001
            run_event "Bengaluru Marathon" 2023 1 100001
            run_event "Bengaluru Marathon" 2024 1 100001
            ;;
        "satara")
            # Satara Hill Half Marathon bib ranges
            #run_event "Satara Hill Half Marathon" 2019 1 100001
            #run_event "Satara Hill Half Marathon" 2021 1 100001
            #run_event "Satara Hill Half Marathon" 2022 1 100001
            run_event "Satara Hill Half Marathon" 2023 1 100001
            ;;
        "hiranandani")
            # Hiranandani Thane Half Marathon
            run_event "Hiranandani Thane Half Marathon 2025" 2025 1 30001
            ;;
        "apla")
            # Apla Pune Marathon
            run_event "Apla Pune Marathon" 2020 1 100001
            run_event "Apla Pune Marathon" 2021 1 100001
            run_event "Apla Pune Marathon" 2022 1 100001
            run_event "Apla Pune Marathon" 2023 1 100001
            run_event "Apla Pune Marathon" 2024 1 100001
            run_event "Apla Pune Marathon" 2025 1 100001
            ;;
        "rotary")
            # Rotary Valsad City Marathon
            run_event "Rotary Valsad City Marathon" 2023 1 30001
            run_event "Rotary Valsad City Marathon" 2024 1 30001
            run_event "Rotary Valsad City Marathon" 2025 1 30001
            ;;
        "ekal")
            # Ekal Run Surat
            run_event "Ekal Run Surat" 2024 1 30001
            run_event "Ekal Run Surat" 2025 1 30001
            ;;
        "bajaj")
            # Bajaj Allianz Half Marathon
            run_event "Bajaj Allianz Pune Half Marathon 2022" 2022 1 100001
            run_event "Bajaj Allianz Pune Half Marathon 2023" 2023 1 100001
            run_event "Bajaj Allianz Pune Half Marathon 2024" 2024 1 100001
            run_event "Bajaj Allianz Pune Half Marathon 2025" 2025 1 100001
            ;;
        "disha")
            # Disha Habitat Bengaluru Runners Jatre
            run_event "Disha Habitat Bengaluru Runners Jatre" 2024 1 60001
            run_event "Disha Habitat Bengaluru Runners Jatre" 2025 1 100001
            ;;
        "indian_navy")
            # Indian Navy Half Marathon
            run_event "Indian Navy Half Marathon" 2020 1 100001
            run_event "Indian Navy Half Marathon" 2021 1 100001
            run_event "Indian Navy Half Marathon" 2022 1 100001
            run_event "Indian Navy Half Marathon" 2023 1 100001
            run_event "Indian Navy Half Marathon" 2024 1 100001
            run_event "Indian Navy Half Marathon" 2025 1 100001
            ;;
        "chandigarh")
            # Chandigarh Fast Marathon
            run_event "Chandigarh Fast Marathon" 2024 1 100001
            run_event "Chandigarh Fast Marathon" 2025 1 100001
            ;;
        "vasai")
            # Vasai Virar Municipal Corporation Marathon
            run_event "Vasai Virar Municipal Corporation Marathon" 2024 1 100001
            run_event "Vasai Virar Municipal Corporation Marathon" 2025 1 100001
            ;;
        "kashmir")
            # Kashmir Marathon
            run_event "Kashmir Marathon" 2024 1 100001
            ;;
        "sbi_patna")
            # SBI Patna Marathon Reloaded
            run_event "SBI Patna Marathon Reloaded" 2024 1 100001
            ;;
        *)
            echo "Invalid event name. Use: ladakh, mumbai, tcs, jbg, vm, bm, satara, hiranandani, apla, rotary, ekal, bajaj, disha, indian_navy, chandigarh, vasai, kashmir, or sbi_patna"
            exit 1
            ;;
    esac
fi

echo "Processing complete. Results saved to $DATA_DIR/" 