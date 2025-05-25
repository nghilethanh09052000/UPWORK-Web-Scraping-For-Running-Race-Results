#source .venv/bin/activate

# Get today's date as YYYYMMDD
today=$(date +"%Y%m%d")
output_dir="data_$today"

# Create the output directory if it doesn't exist
mkdir -p "$output_dir"

while IFS=, read -r master_id; do
    # Skip header row
    if [ "$master_id" = "master_event_id" ]; then
        continue
    fi

    # Skip empty lines
    if [ -z "$master_id" ]; then
        continue
    fi

    echo "Processing master event ID: $master_id"

    Save output in dated folder
    scrapy crawl athlinks_master -a master_id="$master_id" \
        -o "${output_dir}/${master_id}.json"

    if [ $? -ne 0 ]; then
        echo "❌ Error processing master event ID: $master_id"
        continue
    fi

done < unique_master_events_1.csv
