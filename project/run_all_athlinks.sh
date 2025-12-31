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
    
    # New events from user request (2015-2025)
    run_master_event "6172" 1000 1050 &   # Bolder Boulder
    run_master_event "20181" 1050 1100 &  # Walt Disney
    run_master_event "35077" 1100 1150 &  # RBC Brooklyn Half
    run_master_event "8432" 1150 1200 &   # Lilac
    run_master_event "11270" 1200 1250 &  # 
    run_master_event "156909" 1250 1300 & # Philadelphia Marathon
    run_master_event "7249" 1300 1350 &    # 
    run_master_event "3321" 1350 1400 &   # Honolulu Marathon
    run_master_event "9036" 1400 1450 &   # Cooper River Bridge Run
    run_master_event "12410" 1450 1500 &  # Disney Wine
    run_master_event "19554" 1500 1550 &  # JP Morgan Corporate Challenge
    run_master_event "99943" 1550 1600 &  # Rock
    run_master_event "137505" 1600 1650 & # Flying Pig Marathon
    run_master_event "375695" 1650 1700 & # rundisney
    run_master_event "1403" 1700 1750 &   # San Francisco Marathon
    run_master_event "122368" 1750 1800 & # Rocky Run
    run_master_event "12354" 1800 1850 &  # NYCRUNS Brooklyn Half Marathon
    run_master_event "1264" 1850 1900 &   # Los Angeles Marathon
    run_master_event "19454" 1900 1950 &  # Rock 'n' Roll LA
    run_master_event "4782" 1950 2000 &   # Twin Cities
    run_master_event "34467" 2000 2050 &  # Annual Pat's Run
    run_master_event "6620" 2050 2100 &   # Dick's Pittsburgh Marathon
    run_master_event "2436" 2100 2150 &   # 
    run_master_event "18749" 2150 2200 &  # Cowtown
    run_master_event "389503" 2200 2250 & # Disneyland Halloween Half Marathon Weekend
    run_master_event "7884" 2250 2300 &   # Ukrop's Monument Avenue 10K
    run_master_event "20447" 2300 2350 &  # BMW Dallas Marathon
    run_master_event "3403" 2350 2400 &   # St. Jude Memphis Marathon
    run_master_event "4987" 2400 2450 &   # Rock 'n' Roll Nashville
    run_master_event "3294" 2450 2500 &   # Life Time Miami Marathon
    run_master_event "28337" 2500 2550 & # Disneyland Half Marathon
    run_master_event "34956" 2550 2600 &  # Grandma's Marathon
    run_master_event "35340" 2600 2650 &  # Richmond
    run_master_event "36310" 2650 2700 &  # Hood To Coast
    run_master_event "20146" 2700 2750 &  # Long Beach
    run_master_event "23987" 2750 2800 &  # Applied Materials Silicon Valley Turkey Trot
    run_master_event "154412" 2800 2850 & # Detroit Free Press Marathon
    run_master_event "127196" 2850 2900 & # Austin Marathon
    run_master_event "8966" 2900 2950 &   # Statesman Capitol 10K
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
        "6172")
            run_master_event "6172" 1000 1050
            ;;
        "20181")
            run_master_event "20181" 1050 1100
            ;;
        "35077")
            run_master_event "35077" 1100 1150
            ;;
        "8432")
            run_master_event "8432" 1150 1200
            ;;
        "11270")
            run_master_event "11270" 1200 1250
            ;;
        "156909")
            run_master_event "156909" 1250 1300
            ;;
        "7249")
            run_master_event "7249" 1300 1350
            ;;
        "3321")
            run_master_event "3321" 1350 1400
            ;;
        "9036")
            run_master_event "9036" 1400 1450
            ;;
        "12410")
            run_master_event "12410" 1450 1500
            ;;
        "19554")
            run_master_event "19554" 1500 1550
            ;;
        "99943")
            run_master_event "99943" 1550 1600
            ;;
        "137505")
            run_master_event "137505" 1600 1650
            ;;
        "375695")
            run_master_event "375695" 1650 1700
            ;;
        "1403")
            run_master_event "1403" 1700 1750
            ;;
        "122368")
            run_master_event "122368" 1750 1800
            ;;
        "12354")
            run_master_event "12354" 1800 1850
            ;;
        "1264")
            run_master_event "1264" 1850 1900
            ;;
        "19454")
            run_master_event "19454" 1900 1950
            ;;
        "4782")
            run_master_event "4782" 1950 2000
            ;;
        "34467")
            run_master_event "34467" 2000 2050
            ;;
        "6620")
            run_master_event "6620" 2050 2100
            ;;
        "2436")
            run_master_event "2436" 2100 2150
            ;;
        "18749")
            run_master_event "18749" 2150 2200
            ;;
        "389503")
            run_master_event "389503" 2200 2250
            ;;
        "7884")
            run_master_event "7884" 2250 2300
            ;;
        "20447")
            run_master_event "20447" 2300 2350
            ;;
        "3403")
            run_master_event "3403" 2350 2400
            ;;
        "4987")
            run_master_event "4987" 2400 2450
            ;;
        "3294")
            run_master_event "3294" 2450 2500
            ;;
        "28337")
            run_master_event "28337" 2500 2550
            ;;
        "34956")
            run_master_event "34956" 2550 2600
            ;;
        "35340")
            run_master_event "35340" 2600 2650
            ;;
        "36310")
            run_master_event "36310" 2650 2700
            ;;
        "20146")
            run_master_event "20146" 2700 2750
            ;;
        "23987")
            run_master_event "23987" 2750 2800
            ;;
        "154412")
            run_master_event "154412" 2800 2850
            ;;
        "127196")
            run_master_event "127196" 2850 2900
            ;;
        "8966")
            run_master_event "8966" 2900 2950
            ;;
        *)
            echo "Invalid master event ID. Use: 34933, 34538, 103050, 34504, 34812, 34792, 34990, 34440, 4476, 3281, 3241, 34524, 34908, 187582, 131958, 34631, 6172, 20181, 35077, 8432, 11270, 156909, 7249, 3321, 9036, 12410, 19554, 99943, 137505, 375695, 1403, 122368, 12354, 1264, 19454, 4782, 34467, 6620, 2436, 18749, 389503, 7884, 20447, 3403, 4987, 3294, 28337, 34956, 35340, 36310, 20146, 23987, 154412, 127196, or 8966"
            exit 1
            ;;
    esac
fi

echo "Processing complete. Results saved to $DATA_DIR/"