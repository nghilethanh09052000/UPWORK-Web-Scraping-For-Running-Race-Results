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
    """Process a single race JSON file and return counts by distance category"""
    try:
        with open(file_path, 'r') as f:
            data = json.load(f)
            
        # Get race info
        race_name = data.get('race_name', '')
        race_date = data.get('race_date', '')
        
        # Group records by distance category
        distance_stats = defaultdict(lambda: {
            'total_records': 0,
            'expected_total': 0,
            'unique_totals': set()
        })
        
        # Process each record
        for record in data.get('data', []):
            distance_category = record.get('distance_category', 'Unknown')
            rank_overall = record.get('rank_overall', '')
            
            distance_stats[distance_category]['total_records'] += 1
            
            if rank_overall:
                total = extract_total_rank(rank_overall)
                if total > 0:
                    distance_stats[distance_category]['unique_totals'].add(total)
        
        # Calculate expected totals for each distance category
        for category in distance_stats:
            distance_stats[category]['expected_total'] = sum(distance_stats[category]['unique_totals'])
        
        return {
            'race_name': race_name,
            'race_date': race_date,
            'distance_stats': distance_stats
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
    data_dir = "check_data_sporttimingsolutions"
    
    # Prepare CSV output
    output_file = "check_data_sporttimingsolutions.csv"
    fieldnames = ['race_name', 'race_date', 'distance_category', 'total_records', 'expected_total', 'is_matching', 'missing_percentage']
    
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
                # Process each distance category
                for distance_category, stats in result['distance_stats'].items():
                    is_matching = stats['total_records'] == stats['expected_total']
                    missing_percentage = calculate_missing_percentage(
                        stats['total_records'], 
                        stats['expected_total']
                    )
                    
                    all_results.append({
                        'race_name': result['race_name'],
                        'race_date': result['race_date'],
                        'distance_category': distance_category,
                        'total_records': stats['total_records'],
                        'expected_total': stats['expected_total'],
                        'is_matching': 'Yes' if is_matching else 'No',
                        'missing_percentage': f"{missing_percentage}%"
                    })
    
    # Sort results by race name, date, and distance category
    all_results.sort(key=lambda x: (x['race_name'], x['race_date'], x['distance_category']))
    
    # Write sorted results to CSV
    with open(output_file, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_results)
        
    print(f"Results written to {output_file}")

if __name__ == "__main__":
    main() 