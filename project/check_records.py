import os
import json
import csv
from collections import defaultdict

def extract_total_rank(rank_str):
    """Extract the total number from rank string (e.g., "159/1360" -> 1360)"""
    try:
        return int(rank_str.split('/')[1])
    except (IndexError, ValueError):
        return 0

def process_race_file(file_path):
    """Process a single race JSON file and return total counts"""
    try:
        with open(file_path, 'r') as f:
            data = json.load(f)
            
        # Get race info
        race_name = data.get('race_name', '')
        race_date = data.get('race_date', '')
        
        # Collect unique totals
        unique_totals = set()
        
        # Process each record
        for record in data.get('data', []):
            rank_overall = record.get('rank_overall', '')
            if rank_overall:
                total = extract_total_rank(rank_overall)
                if total > 0:
                    unique_totals.add(total)
        
        # Calculate expected total by summing all unique totals
        expected_total = sum(unique_totals)
        
        return {
            'race_name': race_name,
            'race_date': race_date,
            'total_records': len(data.get('data', [])),
            'expected_total': expected_total
        }
    except Exception as e:
        print(f"Error processing {file_path}: {str(e)}")
        return None

def calculate_missing_percentage(record_count, expected_total):
    """Calculate the percentage of missing records"""
    if expected_total == 0:
        return 0
    return round(((expected_total - record_count) / expected_total) * 100, 2)

def main():
    # Directory containing race data
    data_dir = "data_check"
    
    # Prepare CSV output
    output_file = "race_records_check.csv"
    fieldnames = ['race_name', 'race_date', 'total_records', 'expected_total', 'is_matching', 'missing_percentage']
    
    # Store all results for sorting
    all_results = []
    
    # Process each race directory
    for race_dir in os.listdir(data_dir):
        race_path = os.path.join(data_dir, race_dir)
        if not os.path.isdir(race_path):
            continue
            
        # Process each JSON file in the race directory
        for json_file in os.listdir(race_path):
            if not json_file.endswith('.json'):
                continue
                
            file_path = os.path.join(race_path, json_file)
            result = process_race_file(file_path)
            
            if result:
                is_matching = result['total_records'] == result['expected_total']
                missing_percentage = calculate_missing_percentage(result['total_records'], result['expected_total'])
                
                all_results.append({
                    'race_name': result['race_name'],
                    'race_date': result['race_date'],
                    'total_records': result['total_records'],
                    'expected_total': result['expected_total'],
                    'is_matching': 'Yes' if is_matching else 'No',
                    'missing_percentage': f"{missing_percentage}%"
                })
    
    # Sort results by race name and date
    all_results.sort(key=lambda x: (x['race_name'], x['race_date']))
    
    # Write sorted results to CSV
    with open(output_file, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_results)

if __name__ == "__main__":
    main() 