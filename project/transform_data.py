import json
import os
from pathlib import Path
from collections import defaultdict

def transform_data():
    # Read the output.json file
    data_file = Path(__file__).parent / "output.json"
    if not data_file.exists():
        print(f"Error: {data_file} not found")
        return

    # Create data directory
    data_dir = Path(__file__).parent / "data"
    data_dir.mkdir(exist_ok=True)

    # Read the entire JSON file
    try:
        with open(data_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except json.JSONDecodeError as e:
        print(f"Error reading JSON file: {str(e)}")
        return

    if not data:
        print("No data found in output.json")
        return

    print(f"Successfully parsed {len(data)} records")

    # Group data by master_event_id and event_id
    grouped_data = defaultdict(lambda: defaultdict(list))
    for item in data:
        try:
            # Get master_event_id from the summary section
            master_id = item['summary']['master_event_id']
            event_id = item['summary']['event_id']
            
            # Print the IDs for verification
            print(f"Processing: master_event_id={master_id}, event_id={event_id}")
            
            grouped_data[master_id][event_id].append(item)
        except KeyError as e:
            print(f"Error processing item: {str(e)}")
            continue

    # Process each master event
    for master_id, events in grouped_data.items():
        if not master_id:
            continue

        print(f"\nProcessing master event: {master_id}")
        
        # Create master event directory
        master_dir = data_dir / str(master_id)
        master_dir.mkdir(exist_ok=True)

        # Process each event
        for event_id, runners in events.items():
            if not runners:
                continue

            print(f"Processing event: {event_id} with {len(runners)} runners")

            # Get event info from the first runner
            first_runner = runners[0]
            event_info = first_runner['summary']

            # Create the transformed data structure
            transformed_data = {
                'event_id': event_id,
                'race_name': event_info['race_name'],
                'race_date': event_info['race_date'],
                'data': []
            }

            # Add all runner data
            for runner in runners:
                try:
                    runner_data = {
                        'bib_number': runner['bib_number'],
                        'distance_category': runner['distance_category'],
                        'runner_name': runner['runner_name'],
                        'gender': runner['gender'],
                        'age_category': runner['age_category'],
                        'finish_time_net': runner['finish_time_net'],
                        'finish_time_gun': runner['finish_time_gun'],
                        'chip_pace': runner['chip_pace'],
                        'rank_overall': runner['rank_overall'],
                        'rank_gender': runner['rank_gender'],
                        'rank_age_category': runner['rank_age_category']
                    }
                    transformed_data['data'].append(runner_data)
                except KeyError as e:
                    print(f"Error processing runner data: {str(e)}")
                    continue

            # Save to file
            output_file = master_dir / f"{event_id}.json"
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(transformed_data, f, ensure_ascii=False, indent=4)
            
            print(f"Saved {output_file} with {len(transformed_data['data'])} runners")

if __name__ == "__main__":
    transform_data() 