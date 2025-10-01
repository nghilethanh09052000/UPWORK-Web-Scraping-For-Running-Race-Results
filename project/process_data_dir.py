import json
import os
from pathlib import Path
from collections import defaultdict
import traceback

def process_data_directory():
    # Get the data directory
    data_dir = Path(__file__).parent / "data_20250930"
    if not data_dir.exists():
        print(f"Error: {data_dir} not found")
        return

    # Create output directory structure
    output_dir = data_dir / "transformed"
    output_dir.mkdir(exist_ok=True)

    # Process each JSON file in the data directory
    for json_file in data_dir.glob("*.json"):
        # Skip transformed directory and only process 4476.json
      
        print(f"\nProcessing file: {json_file.name}")
        
        try:
            # Read the JSON file
            with open(json_file, 'r', encoding='utf-8') as f:
                try:
                    data = json.load(f)
                except json.JSONDecodeError as e:
                    print(f"Error reading {json_file}: {str(e)}")
                    print(f"Error location: {e.pos}")
                    # Try to read the problematic part
                    with open(json_file, 'r', encoding='utf-8') as f:
                        content = f.read()
                        start = max(0, e.pos - 100)
                        end = min(len(content), e.pos + 100)
                        print(f"Problematic content around position {e.pos}:")
                        print(content[start:end])
                    continue

            if not data:
                print(f"No data found in {json_file}")
                continue

            # Group data by event_id
            grouped_data = defaultdict(list)
            for item in data:
                try:
                    if 'summary' not in item:
                        print(f"Warning: Item missing 'summary' field in {json_file}")
                        continue
                    event_id = item['summary']['event_id']
                    grouped_data[event_id].append(item)
                except KeyError as e:
                    print(f"Error processing item in {json_file}: {str(e)}")
                    print(f"Problematic item: {item}")
                    continue
                except Exception as e:
                    print(f"Unexpected error processing item in {json_file}: {str(e)}")
                    print(traceback.format_exc())
                    continue

            # Process each event
            for event_id, runners in grouped_data.items():
                if not runners:
                    continue

                print(f"Processing event: {event_id} with {len(runners)} runners")

                try:
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
                                #'event_course_id': runner['event_course_id'],
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
                                'rank_age_category': runner['rank_age_category'],
                                'jsonb': {
                                    "bib": runner['jsonb']['bib'],
                                    "nationality": runner['jsonb']['nationality'],
                                    "age": runner['jsonb']['age'],
                                    "race_category": runner['jsonb']['race_category'],
                                    "original_name": runner['jsonb']['original_name'],
                                    "overall_rank": runner['jsonb']['overall_rank'],
                                    "sex": runner['jsonb']['gender'],
                                    "net_time": runner['jsonb']['net_time'],
                                    "gross_time": runner['jsonb']['gross_time'],
                                }
                            }
                            transformed_data['data'].append(runner_data)
                        except KeyError as e:
                            print(f"Error processing runner data: {str(e)}")
                            print(f"Problematic runner: {runner}")
                            continue

                    # Create master event directory
                    master_id = json_file.stem  # Get master_id from filename
                    master_dir = output_dir / str(master_id)
                    master_dir.mkdir(exist_ok=True)

                    # Save to file
                    output_file = master_dir / f"{event_id}.json"
                    with open(output_file, 'w', encoding='utf-8') as f:
                        json.dump(transformed_data, f, ensure_ascii=False, indent=4)
                    
                    print(f"Saved {output_file} with {len(transformed_data['data'])} runners")

                except Exception as e:
                    print(f"Error processing event {event_id} in {json_file}: {str(e)}")
                    print(traceback.format_exc())
                    continue

        except Exception as e:
            print(f"Unexpected error processing {json_file}: {str(e)}")
            print(traceback.format_exc())
            continue

if __name__ == "__main__":
    process_data_directory() 