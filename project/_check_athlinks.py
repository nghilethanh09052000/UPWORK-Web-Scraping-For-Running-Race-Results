import json
import os
import csv
from pathlib import Path
from collections import defaultdict
import requests
from datetime import datetime
import argparse

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

def get_api_counts(master_event_id, min_year=None, max_year=None):
    """Get runner counts from the API
    
    Args:
        master_event_id: The master event ID
        min_year: Minimum year to filter (inclusive). If None, no minimum filter.
        max_year: Maximum year to filter (inclusive). If None, no maximum filter.
    """
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
            
            # Skip if no date
            if not race_date_str:
                continue
                
            try:
                race_date = datetime.fromisoformat(race_date_str)
                race_year = race_date.year
                
                # Apply year filtering if provided
                if min_year is not None and race_year < min_year:
                    continue
                if max_year is not None and race_year > max_year:
                    continue
                    
                api_counts[race_id] = result_count
            except (ValueError, AttributeError):
                # Skip races with invalid dates
                continue
            
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

def compare_counts(min_year=None, max_year=None):
    """Compare local counts with API counts for all master events
    
    Args:
        min_year: Minimum year to filter (inclusive). If None, no minimum filter.
        max_year: Maximum year to filter (inclusive). If None, no maximum filter.
    """
    master_event_data = process_local_data()
    
    results = []

    print("\nComparison Results:")
    if min_year is not None or max_year is not None:
        year_filter = f" (Filtered: {min_year or 'Any'} - {max_year or 'Any'})"
    else:
        year_filter = " (All years)"
    print(f"Year Filter:{year_filter}")
    print("=" * 100)
    print(f"{'Master Event ID':<15} {'Event ID':<10} {'Local Count':<12} {'API Count':<10} {'Missing?':<10} {'% Missing':<10}")
    print("-" * 100)
    
    total_local = 0
    total_api = 0
    
    # Process each master event
    for master_event_id in sorted(master_event_data.keys()):
        local_counts = master_event_data[master_event_id]
        api_counts = get_api_counts(master_event_id, min_year=min_year, max_year=max_year)
        
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
    parser = argparse.ArgumentParser(
        description="Compare local Athlinks data with API counts",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Get all years (no filtering)
  python _check_athlinks.py
  
  # Filter years from 2015 to 2025
  python _check_athlinks.py --min-year 2015 --max-year 2025
  
  # Filter from 2015 onwards (no maximum)
  python _check_athlinks.py --min-year 2015
  
  # Filter up to 2025 (no minimum)
  python _check_athlinks.py --max-year 2025
        """
    )
    parser.add_argument(
        '--min-year',
        type=int,
        default=None,
        help='Minimum year to filter (inclusive). If not provided, no minimum filter is applied.'
    )
    parser.add_argument(
        '--max-year',
        type=int,
        default=None,
        help='Maximum year to filter (inclusive). If not provided, no maximum filter is applied.'
    )
    
    args = parser.parse_args()
    
    compare_counts(min_year=args.min_year, max_year=args.max_year)
