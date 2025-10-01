import scrapy
import json
import base64
import logging
from datetime import datetime

class SportsTimingSolutionsSpider(scrapy.Spider):
    name = 'sportstimingsolutions'
    custom_settings = {
        'AUTOTHROTTLE_ENABLED': False,
        'CONCURRENT_REQUESTS': 200,
        'RETRY_ENABLED': True,
        'LOG_FILE': f"logs/sportstimingsolutions.log",
        'USER_AGENT': "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36 Edg/136.0.0.0",
        'DOWNLOADER_MIDDLEWARES': {
            #'project.middlewares.CustomProxyMiddleware': 543,
        }
    }

    def __init__(self, *args, **kwargs):
        super(SportsTimingSolutionsSpider, self).__init__(*args, **kwargs)
        self.processed_bibs = set()
        self.events = {
            'Ladakh Marathon': list(range(2013, 2024)),  # 2013-2023
            'Hyderabad Marathon': list(range(2011, 2024)),  # 2011-2023
            'New Delhi Marathon': list(range(2016, 2024)),  # 2016-2023
            'SKF Goa Marathon': list(range(2021, 2024)),  # 2021-2023
            # New events added
            'Hiranandani Thane Half Marathon 2025': [2025],
            'Apla Pune Marathon': list(range(2020, 2026)),  # 2020-2025
            'Rotary Valsad City Marathon': list(range(2023, 2026)),  # 2023-2025
            'Ekal Run Surat': list(range(2024, 2026)),  # 2024-2025
            'Bajaj Allianz Pune Half Marathon 2022': [2022],
            'Bajaj Allianz Pune Half Marathon 2023': [2023],
            'Bajaj Allianz Pune Half Marathon 2024': [2024],
            'Bajaj Allianz Pune Half Marathon 2025': [2025],
            'Disha Habitat Bengaluru Runners Jatre': list(range(2024, 2026)),  # 2024-2025
            'Mumbai Marathon': [2020],
            'TCS World 10K Bengaluru': [2025],
            'Satara Hill Half Marathon': [2023],
            'Vadodara Marathon': list(range(2023, 2026)),  # 2023-2025
            'Bengaluru Marathon': [2019, 2021, 2023, 2024],
            # Additional new events
            'Indian Navy Half Marathon': list(range(2020, 2026)),  # 2020-2025
            'Chandigarh Fast Marathon': list(range(2024, 2026)),  # 2024-2025
            'Vasai Virar Municipal Corporation Marathon': list(range(2024, 2026)),  # 2024-2025
            'Kashmir Marathon': [2024],
            'SBI Patna Marathon Reloaded': [2024]
        }
        self.current_event = None
        self.current_year = None
        # Get bib range parameters
        self.start_bib = int(getattr(self, 'start_bib', 1000))
        self.end_bib = int(getattr(self, 'end_bib', 100001))

    def start_requests(self):
        self.event_name = getattr(self, 'event_name', None)
        self.year = getattr(self, 'year', None)
        
        if self.event_name and self.year:
            self.logger.info(f"Starting spider for event: {self.event_name}, year: {self.year}")
            self.logger.info(f"Bib range: {self.start_bib} to {self.end_bib}")
            yield self.make_event_request(self.event_name, self.year)
        else:
            self.logger.error("Event name and year are required")
            return

    def make_event_request(self, event_name, year):
        search_url = f"https://sportstimingsolutions.in/frontend/api/event-search?name={event_name.replace(' ', '+')}&year={year}"
        headers = {
            'accept': 'application/json, text/plain, */*',
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36 Edg/136.0.0.0',
            'referer': 'https://sportstimingsolutions.in/'
        }

        self.logger.info(f"Searching for event: {event_name} {year}")
        
        return scrapy.Request(
            url=search_url,
            headers=headers,
            callback=self.parse_event_search,
            meta={
                'event_name': event_name,
                'year': year
            },
            dont_filter=True
        )

    def parse_event_search(self, response):
        try:
            # Decode base64 response
            encoded_data = response.json().get("data")
            decoded_bytes = base64.b64decode(encoded_data)
            decoded_json = json.loads(decoded_bytes)
            
            self.logger.info(f"Event search response: {decoded_json}")
            
            events = decoded_json.get('events', [])
            if not events:
                self.logger.error(f"No events found for: {response.meta['event_name']} {response.meta['year']}")
                return

            # Get the first event that matches our search
            for event in events:
                event_id = event.get('event_id')
                if not event_id:
                    self.logger.error("No event ID found in response")
                    continue

                self.logger.info(f"Found event ID: {event_id}")

                # Start fetching bibs from event-bibs API using bib number range
                for bib_no in range(self.start_bib, self.end_bib):
                    bibs_url = f"https://sportstimingsolutions.in/frontend/api/event-bibs?event_id={event_id}&term={bib_no}"
                    headers = {
                        'accept': 'application/json, text/plain, */*',
                        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36 Edg/136.0.0.0',
                        'referer': 'https://sportstimingsolutions.in/'
                    }

                    yield scrapy.Request(
                        url=bibs_url,
                        headers=headers,
                        callback=self.parse_event_bibs,
                        meta={
                            'event_id': event_id,
                            'event_name': event.get('name', response.meta['event_name']),
                            'year': response.meta['year'],
                            'bib_no': str(bib_no)
                        },
                        dont_filter=True
                    )

        except Exception as e:
            self.logger.error(f"Error in parse_event_search: {str(e)}")

    def parse_event_bibs(self, response):
        try:
            # Decode base64 response
            encoded_data = response.json().get("data")
            if not encoded_data:
                self.logger.info(f"No data found for bib: {response.meta['bib_no']}")
                return

            decoded_bytes = base64.b64decode(encoded_data)
            decoded_json = json.loads(decoded_bytes)
            
            participants = decoded_json.get('participants', [])
            if not participants:
                self.logger.info(f"No participants found for bib: {response.meta['bib_no']}")
                return

            self.logger.info(f"Found {len(participants)} participants for bib: {response.meta['bib_no']}")

            # Process each participant's bib
            for participant in participants:
                bib_no = participant.get('bibno')
                if not bib_no or str(bib_no) in self.processed_bibs:
                    continue

                self.processed_bibs.add(str(bib_no))
                self.logger.info(f"Processing bib: {bib_no}")

                # Request individual bib results
                bib_url = f"https://sportstimingsolutions.in/frontend/api/event/bib/result?event_id={response.meta['event_id']}&bibNo={bib_no}"
                headers = {
                    'accept': 'application/json, text/plain, */*',
                    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36 Edg/136.0.0.0',
                    'referer': 'https://sportstimingsolutions.in/'
                }

                yield scrapy.Request(
                    url=bib_url,
                    headers=headers,
                    callback=self.parse_bib_result,
                    meta={
                        'event_id': response.meta['event_id'],
                        'event_name': response.meta['event_name'],
                        'year': response.meta['year'],
                        'bib_no': str(bib_no)
                    },
                    dont_filter=True
                )

        except Exception as e:
            self.logger.error(f"Error in parse_event_bibs: {str(e)}")

    def parse_bib_result(self, response):
        try:
            # Decode base64 response
            encoded_data = response.json().get("data")
            if not encoded_data:
                self.logger.info(f"No data found for bib: {response.meta['bib_no']}, skipping...")
                return

            try:
                decoded_bytes = base64.b64decode(encoded_data)
                decoded_json = json.loads(decoded_bytes)
            except (base64.binascii.Error, json.JSONDecodeError) as e:
                self.logger.info(f"Invalid data format for bib: {response.meta['bib_no']}, skipping... Error: {str(e)}")
                return
            
            bib_no = response.meta['bib_no']
            self.logger.info(f"Processing bib result for {bib_no}")
            
            if not decoded_json:
                self.logger.info(f"Empty decoded data for bib: {bib_no}, skipping...")
                return

            # Add to processed bibs
            self.processed_bibs.add(bib_no)

            # Extract event and race info
            event = decoded_json.get('event', {})
            race = decoded_json.get('race', {})
            participant = decoded_json.get('participant', {})
            brackets = decoded_json.get('brackets', [])
            intervals = decoded_json.get('intervals', [])
            gender = 'Male' if 'M' in participant.get('gender', '') else 'Female'

            # Skip if no participant data
            if not participant:
                self.logger.info(f"No participant data for bib: {bib_no}, skipping...")
                return

            # Get rankings from brackets
            overall_rank = None
            gender_rank = None
            category_rank = None
            chip_pace = None
            
            for bracket in brackets:
                if bracket.get('bracket_name') == 'Overall':
                    overall_rank = f"{bracket.get('bracket_rank')}/{bracket.get('bracket_participants')}"
                elif bracket.get('bracket_name') == gender:
                    gender_rank = f"{bracket.get('bracket_rank')}/{bracket.get('bracket_participants')}"
                # If this is the third bracket and it's not Overall or gender, it's likely the category rank
                elif len(brackets) >= 3 and brackets.index(bracket) == 2:
                    category_rank = f"{bracket.get('bracket_rank')}/{bracket.get('bracket_participants')}"

            for interval in intervals:
                if interval.get('interval_name') == 'Full Course':
                    chip_pace = interval.get('chip_pace', '')

            result = {
                'summary': {
                    'event_id': event.get('id'),
                    'race_name': event.get('name'),
                    'race_date': race.get('race_date'),
                    'master_event_id': event.get('event_id')
                },
                'bib_number': participant.get('bibno'),
                'distance_category': race.get('name', ''),
                'runner_name': participant.get('full_name', ''),
                'gender': gender,
                'age_category': '',  
                'finish_time_net': decoded_json.get('finished_time', ''),
                'finish_time_gun': decoded_json.get('gun_time', ''),
                'chip_pace': chip_pace or '',  
                'rank_overall': overall_rank or '',
                'rank_gender': gender_rank or '',
                'rank_age_category': category_rank or '',
                'jsonb': {
                    'bib': participant.get('bibno'),
                    'nationality': participant.get('country_code', ''),
                    'age': '',  
                    'race_category': race.get('name', ''),
                    'original_name': participant.get('full_name', ''),
                    'overall_rank': overall_rank.split('/')[0] if overall_rank else '',
                    'gender': gender,
                    'net_time': decoded_json.get('finished_time', ''),
                    'gross_time': decoded_json.get('gun_time', '')
                }
            }

            yield result

        except Exception as e:
            self.logger.error(f"Error in parse_bib_result for bib {response.meta['bib_no']}: {str(e)}") 