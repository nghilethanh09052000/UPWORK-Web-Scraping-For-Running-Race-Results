import scrapy
import json
import logging
from datetime import datetime
import re

class IfinishSpider(scrapy.Spider):
    name = 'ifinish'
    custom_settings = {
        # 'CONCURRENT_REQUESTS': 50,
        # 'CONCURRENT_REQUESTS_PER_DOMAIN': 32,
        # 'DOWNLOAD_DELAY': 0,
        'RETRY_ENABLED': True,
        'LOG_FILE': f"logs/ifinish.log",
        'USER_AGENT': "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36 Edg/136.0.0.0"
    }

    def __init__(self, *args, **kwargs):
        super(IfinishSpider, self).__init__(*args, **kwargs)
        self.processed_bibs = set()
        self.events = {
            'Ladakh Marathon': list(range(2013, 2020)),  # 2013-2019
            'Hyderabad Marathon': list(range(2011, 2024)),  # 2011-2023
            'New Delhi Marathon': list(range(2016, 2020)),  # 2016-2019
            'SKF Goa Marathon': list(range(2021, 2024))  # 2021-2023
        }
        self.current_event = None
        self.current_year = None

    def format_time(self, milliseconds):
        if not milliseconds:
            return ''
        seconds = int(milliseconds)
        hours = seconds // 3600
        minutes = (seconds % 3600) // 60
        seconds = seconds % 60
        return f"{hours:02d}:{minutes:02d}:{seconds:02d}"

    def start_requests(self):
        # Get event name and year from command line arguments
        self.event_name = getattr(self, 'event_name', None)
        self.year = getattr(self, 'year', None)
        
        if self.event_name and self.year:
            # Single event mode
            self.logger.info(f"Starting spider for event: {self.event_name}, year: {self.year}")
            yield self.make_event_request(self.event_name, self.year)
        else:
            # Multiple events mode
            for event_name, years in self.events.items():
                for year in years:
                    self.logger.info(f"Starting spider for event: {event_name}, year: {year}")
                    yield self.make_event_request(event_name, year)

    def make_event_request(self, event_name, year):
        search_url = "https://api2.ifinish.in/api/searchTimingEvent"
        headers = {
            'accept': 'application/json, text/plain, */*',
            'content-type': 'application/json',
            'apikey': '2ad86174fdcde3cea624bd9c29cb5a0e1818eda0bb3af3d5fe6328e956c1457b',
            'origin': 'https://ifinish.in',
            'referer': 'https://ifinish.in/'
        }
        payload = {
            "eventName": event_name,
            "year": str(year)
        }

        self.logger.info(f"Searching for event with payload: {payload}")
        
        return scrapy.Request(
            url=search_url,
            method='POST',
            headers=headers,
            body=json.dumps(payload),
            callback=self.parse_event_search,
            meta={
                'event_name': event_name,
                'year': year
            },
            dont_filter=True
        )

    def parse_event_search(self, response):
        try:
            data = json.loads(response.text)
            self.logger.info(f"Event search response: {data}")
            
            if not data.get('status'):
                self.logger.error(f"Failed to find event: {response.meta['event_name']}")
                return

            # The API returns a list of events in data
            events = data.get('data', [])
            if not events:
                self.logger.error("No events found in response")
                return

            # Get the first event that matches our search
            for event in events:
 
                event_id = event.get('id')
                if not event_id:
                    self.logger.error("No event ID found in response")
                    return

                self.logger.info(f"Found event ID: {event_id}")

                # Request top timing results
                results_url = "https://api2.ifinish.in/api/topTimingResults"
                headers = {
                    'accept': 'application/json, text/plain, */*',
                    'content-type': 'application/json',
                    'apikey': '7db0f40612e7d0eb1a80773b34784f5f8521e99e51709ddfc5500373627a1bb6',
                    'origin': 'https://ifinish.in',
                    'referer': 'https://ifinish.in/'
                }
                payload = {
                    "eventId": event_id
                }

                self.logger.info(f"Requesting top results with payload: {payload}")

                yield scrapy.Request(
                    url=results_url,
                    method='POST',
                    headers=headers,
                    body=json.dumps(payload),
                    callback=self.parse_top_results,
                    meta={
                        'event_id': event_id,
                        'event_name': event.get('name', response.meta['event_name']),
                        'year': response.meta['year'],
                        'output_filename': f"{event_id}.json"  # Use only event ID for filename
                    },
                    dont_filter=True
                )
        except Exception as e:
            self.logger.error(f"Error in parse_event_search: {str(e)}")

    def parse_top_results(self, response):
        try:
            data = json.loads(response.text)
            self.logger.info(f"Top results response: {data}")
            
            if not data.get('status'):
                self.logger.error("Failed to get top results")
                return

            event_id = response.meta['event_id']
            event_name = response.meta['event_name']
            year = response.meta['year']
            string_patterns = set()  # Store all unique patterns found

            # Process each category
            for category in data.get('data', {}).get('categoryResults', []):
                category_name = category.get('name', '')
                self.logger.info(f"Processing category: {category_name}")
                
                # Process top results
                for top_result in category.get('topResults', []):
                    gender = 'Male' if 'Male' in top_result.get('title', '') else 'Female'
                    
                    for result in top_result.get('results', []):
                        bib = result.get('bib', '')
                        if not bib or bib in self.processed_bibs:
                            self.logger.info(f"bib {bib} already processed, skipping")
                            continue

                        # Extract prefix pattern from bib (e.g., "F", "H", "T", "M" with or without hyphen)
                        prefix_match = re.match(r'^([A-Z])(?:-)?', str(bib))
                        if prefix_match and prefix_match.group(1):
                            prefix = prefix_match.group(1)
                            string_patterns.add(prefix)  # Add without hyphen
                            string_patterns.add(f"{prefix}-")  # Add with hyphen
                            self.logger.info(f"Found bib prefix patterns: {prefix} and {prefix}-")

                        self.processed_bibs.add(bib)
                        self.logger.info(f"Processing top result bib: {bib}")

                        # Request individual bib results
                        bib_url = "https://api2.ifinish.in/api/getBibResults"
                        headers = {
                            'accept': 'application/json, text/plain, */*',
                            'content-type': 'application/json',
                            'apikey': '497457a31d0e4080a8fa6dd0c4154e47534c4b34407193e7f63070b1b08bf711',
                            'origin': 'https://ifinish.in',
                            'referer': 'https://ifinish.in/'
                        }
                        payload = {
                            "bib": bib,
                            "eventId": event_id
                        }

                        yield scrapy.Request(
                            url=bib_url,
                            method='POST',
                            headers=headers,
                            body=json.dumps(payload),
                            callback=self.parse_bib_result,
                            meta={
                                'event_id': event_id,
                                'event_name': event_name,
                                'category_name': category_name,
                                'bib': bib,
                                'year': year
                            },
                            dont_filter=True
                        )

                # Process subCategoryResults
                for sub_category in category.get('subCategoryResults', []):
                    sub_category_name = sub_category.get('title', '')
                    self.logger.info(f"Processing sub-category: {sub_category_name}")
                    
                    for result in sub_category.get('results', []):
                        bib = result.get('bib', '')
                        if not bib or bib in self.processed_bibs:
                            self.logger.info(f"bib {bib} already processed, skipping")
                            continue

                        # Extract prefix pattern from bib (e.g., "F", "H", "T", "M" with or without hyphen)
                        prefix_match = re.match(r'^([A-Z])(?:-)?', str(bib))
                        if prefix_match and prefix_match.group(1):
                            prefix = prefix_match.group(1)
                            string_patterns.add(prefix)  # Add without hyphen
                            string_patterns.add(f"{prefix}-")  # Add with hyphen
                            self.logger.info(f"Found bib prefix patterns: {prefix} and {prefix}-")

                        self.processed_bibs.add(bib)
                        self.logger.info(f"Processing sub-category bib: {bib}")

                        # Request individual bib results
                        bib_url = "https://api2.ifinish.in/api/getBibResults"
                        headers = {
                            'accept': 'application/json, text/plain, */*',
                            'content-type': 'application/json',
                            'apikey': '497457a31d0e4080a8fa6dd0c4154e47534c4b34407193e7f63070b1b08bf711',
                            'origin': 'https://ifinish.in',
                            'referer': 'https://ifinish.in/'
                        }
                        payload = {
                            "bib": bib,
                            "eventId": event_id
                        }

                        yield scrapy.Request(
                            url=bib_url,
                            method='POST',
                            headers=headers,
                            body=json.dumps(payload),
                            callback=self.parse_bib_result,
                            meta={
                                'event_id': event_id,
                                'event_name': event_name,
                                'category_name': category_name,
                                'bib': bib,
                                'year': year
                            },
                            dont_filter=True
                        )

            # After processing all top results, start bib search
            self.logger.info("Starting bib search requests")
            
            # First do normal numeric search
            for bib_prefix in range(100, 1001):
                bib_search_url = "https://api2.ifinish.in/api/searchTimingBibNumber"
                headers = {
                    'accept': 'application/json, text/plain, */*',
                    'accept-language': 'en-US,en;q=0.9',
                    'apikey': 'ca5ae6f17b5ddb305ad32be93e50e7345ba8f462a794a99f9656862fdcf5702b',
                    'content-type': 'application/json',
                    'origin': 'https://ifinish.in',
                    'referer': 'https://ifinish.in/',
                    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36 Edg/136.0.0.0'
                }
                payload = {
                    "bib": str(bib_prefix),
                    "eventId": event_id
                }

                yield scrapy.Request(
                    url=bib_search_url,
                    method='POST',
                    headers=headers,
                    body=json.dumps(payload),
                    callback=self.parse_bib_search,
                    meta={
                        'event_id': event_id,
                        'event_name': event_name,
                        'year': year
                    },
                    dont_filter=True
                )

            # If we found any prefix patterns, do additional searches with each pattern
            if string_patterns:
                self.logger.info(f"Starting bib search requests with patterns: {string_patterns}")
                for pattern in string_patterns:
                    self.logger.info(f"Starting bib search requests with pattern: {pattern}")
                    for bib_prefix in range(100, 1001):
                        bib_search_url = "https://api2.ifinish.in/api/searchTimingBibNumber"
                        headers = {
                            'accept': 'application/json, text/plain, */*',
                            'accept-language': 'en-US,en;q=0.9',
                            'apikey': 'ca5ae6f17b5ddb305ad32be93e50e7345ba8f462a794a99f9656862fdcf5702b',
                            'content-type': 'application/json',
                            'origin': 'https://ifinish.in',
                            'referer': 'https://ifinish.in/',
                            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36 Edg/136.0.0.0'
                        }
                        payload = {
                            "bib": f"{pattern}{bib_prefix}",
                            "eventId": event_id
                        }

                        yield scrapy.Request(
                            url=bib_search_url,
                            method='POST',
                            headers=headers,
                            body=json.dumps(payload),
                            callback=self.parse_bib_search,
                            meta={
                                'event_id': event_id,
                                'event_name': event_name,
                                'year': year
                            },
                            dont_filter=True
                        )

        except Exception as e:
            self.logger.error(f"Error in parse_top_results: {str(e)}")

    def parse_bib_search(self, response):
        data = json.loads(response.text)
        self.logger.info(f"Processing bib search response")
        
        if not data.get('status'):
            self.logger.error(f"Failed to get bib search results")
            return

        event_id = response.meta['event_id']
        event_name = response.meta['event_name']
        year = response.meta['year']

        # Process each result from the bib search
        for result in data.get('data', []):
            bib = result.get('bib', '')
            if not bib or bib in self.processed_bibs:
                continue

            self.processed_bibs.add(bib)
            self.logger.info(f"Processing bib search result: {bib}")

            # Request individual bib results
            bib_url = "https://api2.ifinish.in/api/getBibResults"
            headers = {
                'accept': 'application/json, text/plain, */*',
                'content-type': 'application/json',
                'apikey': '497457a31d0e4080a8fa6dd0c4154e47534c4b34407193e7f63070b1b08bf711',
                'origin': 'https://ifinish.in',
                'referer': 'https://ifinish.in/'
            }
            payload = {
                "bib": bib,
                "eventId": event_id
            }

            yield scrapy.Request(
                url=bib_url,
                method='POST',
                headers=headers,
                body=json.dumps(payload),
                callback=self.parse_bib_result,
                meta={
                    'event_id': event_id,
                    'event_name': event_name,
                    'category_name': result.get('event', ''),
                    'bib': bib,
                    'year': year
                },
                dont_filter=True
            )

    def parse_bib_result(self, response):
        try:
            data = json.loads(response.text)
            year = response.meta['year']
            bib = response.meta['bib']
            self.logger.info(f"Processing bib result for {bib}")
            
            if not data.get('status'):
                self.logger.error(f"Failed to get results for bib: {bib}")
                return

            result_data = data.get('data', {})
            
            # Extract rank information
            rank_parts = str(result_data.get('Rank', '')).split('/')
            overall_rank = rank_parts[0].strip() if len(rank_parts) > 0 else ''
            total_athletes = rank_parts[1].strip() if len(rank_parts) > 1 else ''

            category_rank_parts = str(result_data.get('Category Rank', '')).split('/')
            category_rank = category_rank_parts[0].strip() if len(category_rank_parts) > 0 else ''
            category_total = category_rank_parts[1].strip() if len(category_rank_parts) > 1 else ''

            gender_rank_parts = str(result_data.get('Gender Rank', '')).split('/')
            gender_rank = gender_rank_parts[0].strip() if len(gender_rank_parts) > 0 else ''
            gender_total = gender_rank_parts[1].strip() if len(gender_rank_parts) > 1 else ''

            # Extract time information
            net_time = result_data.get('Net Time', '').split(',')[0] if result_data.get('Net Time') else ''
            gross_time = result_data.get('Gross Time', '')

            result = {
                'summary': {
                    'event_id': response.meta['event_id'],
                    'race_name': response.meta['event_name'],
                    'race_date': year,
                    'master_event_id': response.meta['event_id']
                },
                'bib_number': bib,
                'distance_category': response.meta['category_name'],
                'runner_name': result_data.get('Name', ''),
                'gender': result_data.get('Gender', ''),
                'age_category': result_data.get('Category', ''),
                'finish_time_net': net_time,
                'finish_time_gun': gross_time,
                'chip_pace': '',
                'rank_overall': f"{overall_rank}/{total_athletes}",
                'rank_gender': f"{gender_rank}/{gender_total}",
                'rank_age_category': f"{category_rank}/{category_total}",
                'jsonb': {
                    'bib': bib,
                    'nationality': '',
                    'age': '',
                    'race_category': response.meta['category_name'],
                    'original_name': result_data.get('Name', ''),
                    'overall_rank': overall_rank,
                    'gender': result_data.get('Gender', ''),
                    'net_time': net_time,
                    'gross_time': gross_time
                }
            }
            yield result

        except Exception as e:
            self.logger.error(f"Error in parse_bib_result for bib {response.meta['bib']}: {str(e)}")
            # Log the failed request payload
            try:
                payload = json.loads(response.request.body)
                self.logger.error(f"Failed request payload for bib {response.meta['bib']}: {payload}")
            except:
                self.logger.error(f"Failed to parse request payload for bib {response.meta['bib']}: {response.request.body}")
