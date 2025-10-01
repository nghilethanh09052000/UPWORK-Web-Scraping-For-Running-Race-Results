import json
import os
import csv
from pathlib import Path
from collections import defaultdict
import requests
from datetime import datetime

def count_runners_in_json(file_path):
    """Count runners in a single JSON file"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            return len(data)
    except Exception as e:
        print(f"Error reading {file_path}: {str(e)}")
        return 0

def process_local_data():
    """Process all JSON files in the data directory"""
    data_dir = Path(__file__).parent / "check_data_athlinks"
    if not data_dir.exists():
        print(f"Error: {data_dir} not found")
        return {}

    # Dictionary to store master_event_id -> {event_id -> runner_count}
    master_event_data = defaultdict(lambda: defaultdict(int))

    # Process each JSON file
    for json_file in data_dir.glob("*.json"):
        if json_file.name == "based.json":  # Skip the API data file
            continue
            
        master_event_id = json_file.stem  # Get master event ID from filename
        print(f"\nProcessing file: {master_event_id}.json")
        
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                
            if not data:
                print(f"No data found in {json_file}")
                continue

            # Group data by event_id
            for item in data:
                try:
                    if 'summary' not in item:
                        continue
                    event_id = str(item['summary']['event_id'])
                    master_event_data[master_event_id][event_id] += 1
                except KeyError:
                    continue

        except Exception as e:
            print(f"Error processing {json_file}: {str(e)}")
            continue

    return master_event_data

def get_api_counts(master_event_id):
    """Get runner counts from the API"""
    api_url = f"https://alaska.athlinks.com/MasterEvents/Api/{master_event_id}"
    
    try:
        response = requests.get(api_url)
        response.raise_for_status()
        data = response.json()
        
        if not data.get('success'):
            print(f"API returned unsuccessful response: {data.get('errorMessage')}")
            return {}
            
        api_counts = {}
        for race in data['result']['eventRaces']:
            race_id = str(race['raceID'])
            result_count = race['resultCount']
            race_date_str = race.get('raceDate', '')
            race_date = datetime.fromisoformat(race_date_str)
            if race_date.year < 2005:
                continue
            api_counts[race_id] = result_count
            
        return api_counts
    except requests.exceptions.RequestException as e:
        print(f"Error making API request: {str(e)}")
        return {}
    except json.JSONDecodeError as e:
        print(f"Error parsing API response: {str(e)}")
        return {}
    except Exception as e:
        print(f"Unexpected error getting API data: {str(e)}")
        return {}

def compare_counts():
    """Compare local counts with API counts for all master events"""
    master_event_data = process_local_data()
    
    results = []

    print("\nComparison Results:")
    print("=" * 100)
    print(f"{'Master Event ID':<15} {'Event ID':<10} {'Local Count':<12} {'API Count':<10} {'Missing?':<10} {'% Missing':<10}")
    print("-" * 100)
    
    total_local = 0
    total_api = 0
    
    # Process each master event
    for master_event_id in sorted(master_event_data.keys()):
        local_counts = master_event_data[master_event_id]
        api_counts = get_api_counts(master_event_id)
        
        # Get all event IDs for this master event
        all_event_ids = set(local_counts.keys()) | set(api_counts.keys())
        sorted_event_ids = sorted(all_event_ids, key=lambda x: int(x))
        
        # Print and collect results for each event
        for event_id in sorted_event_ids:
            local_count = local_counts.get(event_id, 0)
            api_count = api_counts.get(event_id, 0)
            percentage_missing =  (api_count - local_count) * 100 / api_count if api_count else 0.0
            is_missing = "No" if local_count == api_count else "Yes"
            print(f"{master_event_id:<15} {event_id:<10} {local_count:<12} {api_count:<10} {is_missing:<10} {percentage_missing:<10.2f}")

            if int(percentage_missing) == 100: continue
            results.append({
                "master_event_id": master_event_id,
                "event_id": event_id,
                "local_count": local_count,
                "api_count": api_count,
                "percentage_missing": percentage_missing,
                "missing": is_missing
            })

            total_local += local_count
            total_api += api_count
    
    # Summary statistics
    print("\nSummary:")
    print(f"Total local runners: {total_local}")
    print(f"Total API runners: {total_api}")
    print(f"Difference: {total_local - total_api}")

    # Export results to CSV
    csv_path = Path(__file__).parent / "check_data_athlinks.csv"
    with open(csv_path, mode='w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=["master_event_id", "event_id", "local_count", "api_count", "percentage_missing", "missing"])
        writer.writeheader()
        writer.writerows(results)

    print(f"\nComparison results exported to {csv_path}")

if __name__ == "__main__":
    compare_counts()
