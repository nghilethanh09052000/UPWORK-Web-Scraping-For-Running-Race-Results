import scrapy
import json
import logging
from datetime import datetime

class RaceTimeSpider(scrapy.Spider):
    name = 'racetime'
    custom_settings = {
        'AUTOTHROTTLE_ENABLED': False,
        'CONCURRENT_REQUESTS': 100,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 100,
        'CONCURRENT_REQUESTS_PER_IP': 100,
        'RETRY_ENABLED': True,
        'LOG_FILE': f"logs/racetime.log",
        'USER_AGENT': "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36 Edg/136.0.0.0"
    }

    def __init__(self, *args, **kwargs):
        super(RaceTimeSpider, self).__init__(*args, **kwargs)
        self.processed_bibs = set()
        # Define available races
        self.available_races = {
            # '2022ndm': '2022 New Delhi Marathon',
            # 'apollo-new-delhi-marathon-2023': 'Apollo Tyres New Delhi Marathon 2023'
            'spbm2022': 'Shriram Properties Bengaluru Marathon 2022',
        }
     
        self.current_event = None
        self.current_year = None

    def start_requests(self):
        race = getattr(self, 'race', None)
        
        if not race or race not in self.available_races:
            self.logger.error(f"Race parameter is required. Available races: {', '.join(self.available_races.keys())}")
            return

        search_url = f"https://appapi.racetime.in/result/events?race={race}"
        headers = {
            'accept': 'application/json, text/plain, */*',
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36 Edg/136.0.0.0',
            'referer': 'https://racetime.in/'
        }

        self.logger.info(f"Starting spider for race: {self.available_races[race]}")
        
        yield scrapy.Request(
            url=search_url,
            headers=headers,
            callback=self.parse_event_search,
            meta={'race': race},
            dont_filter=True
        )

    def parse_event_search(self, response):
        try:
            data = response.json().get("data")
            if not data:
                self.logger.error("No data found for event")
                return

            event_id = data.get('id')
            if not event_id:
                self.logger.error("No event ID found in response")
                return

            self.logger.info(f"Found event ID: {event_id}")

            # Get events list
            events = data.get('events', [])
            if not events:
                self.logger.error("No events found")
                return

            # Process each event
            for event in events:
                # Get results for each gender
                for gender in ['OVERALL', 'MALE', 'FEMALE']:
                    results_url = f"https://appapi.racetime.in/result/list?raceID={event_id}&event={event}&gender={gender}&page=1"
                    headers = {
                        'accept': 'application/json, text/plain, */*',
                        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36 Edg/136.0.0.0',
                        'referer': 'https://racetime.in/'
                    }

                    yield scrapy.Request(
                        url=results_url,
                        headers=headers,
                        callback=self.parse_results_list,
                        meta={
                            'event_id': event_id,
                            'event_name': data.get('name'),
                            'year': '2022',
                            'event': event,
                            'gender': gender,
                            'page': 1
                        },
                        dont_filter=True
                    )

        except Exception as e:
            self.logger.error(f"Error in parse_event_search: {str(e)}")

    def parse_results_list(self, response):
        try:
            data = response.json().get("data")
            if not data:
                self.logger.info(f"No data found for page: {response.meta['page']}")
                return

            results = data.get('results', [])
            if not results:
                self.logger.info(f"No results found for page: {response.meta['page']}")
                return

            self.logger.info(f"Found {len(results)} results for page: {response.meta['page']}")

            # Process each result
            for result in results:
                bib_no = result.get('bibNo')
                if not bib_no or str(bib_no) in self.processed_bibs:
                    continue

                self.processed_bibs.add(str(bib_no))
                self.logger.info(f"Processing bib: {bib_no}")

                # Request individual bib details
                bib_url = f"https://appapi.racetime.in/result/details?raceID={response.meta['event_id']}&event={response.meta['event']}&bibNo={bib_no}&name={result.get('name', '').replace(' ', '+')}"
                headers = {
                    'accept': 'application/json, text/plain, */*',
                    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36 Edg/136.0.0.0',
                    'referer': 'https://racetime.in/'
                }

                yield scrapy.Request(
                    url=bib_url,
                    headers=headers,
                    callback=self.parse_bib_details,
                    meta={
                        'event_id': response.meta['event_id'],
                        'event_name': response.meta['event_name'],
                        'year': response.meta['year'],
                        'event': response.meta['event'],
                        'bib_no': str(bib_no)
                    },
                    dont_filter=True
                )

            # Check if there are more pages
            if data.get('hasNextPage'):
                next_page = response.meta['page'] + 1
                next_url = f"https://appapi.racetime.in/result/list?raceID={response.meta['event_id']}&event={response.meta['event']}&gender={response.meta['gender']}&page={next_page}"
                headers = {
                    'accept': 'application/json, text/plain, */*',
                    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36 Edg/136.0.0.0',
                    'referer': 'https://racetime.in/'
                }

                yield scrapy.Request(
                    url=next_url,
                    headers=headers,
                    callback=self.parse_results_list,
                    meta={
                        'event_id': response.meta['event_id'],
                        'event_name': response.meta['event_name'],
                        'year': response.meta['year'],
                        'event': response.meta['event'],
                        'gender': response.meta['gender'],
                        'page': next_page
                    },
                    dont_filter=True
                )

        except Exception as e:
            self.logger.error(f"Error in parse_results_list: {str(e)}")

    def parse_bib_details(self, response):
        try:
            data = response.json().get("data")
            if not data:
                self.logger.info(f"No data found for bib: {response.meta['bib_no']}, skipping...")
                return

            bib_no = response.meta['bib_no']
            self.logger.info(f"Processing bib details for {bib_no}")

            # Add to processed bibs
            self.processed_bibs.add(bib_no)

            # Extract event and race info
            gender = 'Male' if data.get('gender') == 'M' else 'Female'

            result = {
                'summary': {
                    'event_id': response.meta['event_id'],
                    'race_name': data.get('raceName'),
                    'race_date': data.get('raceDate'),
                    'master_event_id': response.meta['event_id']
                },
                'bib_number': data.get('bibNo'),
                'distance_category': data.get('event'),
                'runner_name': data.get('name'),
                'gender': gender,
                'age_category': data.get('category', ''),
                'finish_time_net': data.get('netTime'),
                'finish_time_gun': data.get('gunTime'),
                'chip_pace': '',  # Not available in the API
                'rank_overall': f"{data.get('overallRank')}/{data.get('overallCount')}" if data.get('overallCount') else str(data.get('overallRank')),
                'rank_gender': f"{data.get('genderRank')}/{data.get('genderCount')}" if data.get('genderCount') else str(data.get('genderRank')),
                'rank_age_category': f"{data.get('categoryRank')}/{data.get('categoryCount')}" if data.get('categoryCount') else str(data.get('categoryRank')),
                'jsonb': {
                    'bib': data.get('bibNo'),
                    'nationality': '',  # Not available in the API
                    'age': '',  # Not available in the API
                    'race_category': data.get('event'),
                    'original_name': data.get('name'),
                    'overall_rank': str(data.get('overallRank')),
                    'gender': gender,
                    'net_time': data.get('netTime'),
                    'gross_time': data.get('gunTime'),
                    'laps': data.get('laps', []),
                    'certificate': data.get('certificate'),
                    'medal': data.get('medal')
                }
            }

            yield result

        except Exception as e:
            self.logger.error(f"Error in parse_bib_details for bib {response.meta['bib_no']}: {str(e)}") 