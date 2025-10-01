import scrapy
import json
import logging
from datetime import datetime
from urllib.parse import urljoin

class RacemateindiaSpider(scrapy.Spider):
    name = 'racemateindia'
    custom_settings = {
        'AUTOTHROTTLE_ENABLED': False,
        'CONCURRENT_REQUESTS': 200,
        # 'DOWNLOADER_MIDDLEWARES': {
        #     'project.middlewares.CustomProxyMiddleware': 543,
        # },
        'RETRY_ENABLED': True,
        'RETRY_TIMES': 3,
        'RETRY_HTTP_CODES': [500, 502, 503, 504, 408, 429, 403],
        'HTTPERROR_ALLOWED_CODES': [403, 404, 500, 502, 503, 504],
        'LOG_FILE': f"logs/racemateindia.log",
        'USER_AGENT': "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36 Edg/136.0.0.0"
    }

    def __init__(self, *args, **kwargs):
        super(RacemateindiaSpider, self).__init__(*args, **kwargs)
        self.processed_bibs = set()
        self.events = {
            'Sarmang Dehradun Marathon': {
                '2023': '62a545ec10c972219c7fc0ce8b3a2dee:4ad0',
                '2024': '584637415441a42e981485ee98b4b9b1:c845e8'
            }
        }
        self.current_event = None
        self.current_year = None
        # Get bib range parameters
        self.start_bib = int(getattr(self, 'start_bib', 1))
        self.end_bib = int(getattr(self, 'end_bib', 99999))
        
        # Define letter prefixes to search through
        self.letter_prefixes = ['A', 'B', 'C', 'D']
        
    def generate_search_inputs(self, start_bib, end_bib):
        """Generate all search inputs combining numbers and letter prefixes"""
        search_inputs = []
        
        for bib in range(start_bib, end_bib + 1):
            # Add number only
            search_inputs.append(str(bib))
            
            # Add letter-prefixed numbers
            for prefix in self.letter_prefixes:
                search_inputs.append(f"{prefix}{bib}")
        
        return search_inputs

    def start_requests(self):
        self.event_name = getattr(self, 'event_name', None)
        self.year = getattr(self, 'year', None)
        
        if self.event_name and self.year:
            self.logger.info(f"Starting spider for event: {self.event_name}, year: {self.year}")
            self.logger.info(f"Bib range: {self.start_bib} to {self.end_bib}")
            
            event_id = self.events.get(self.event_name, {}).get(self.year)
            if not event_id:
                self.logger.error(f"Event ID not found for {self.event_name} {self.year}")
                return
                
            # Generate all search inputs
            search_inputs = self.generate_search_inputs(self.start_bib, self.end_bib)
            
            # Start searching for bibs
       
            for search_input in search_inputs:
                search_url = f"https://racemateindia.com/leaderboard/search?event_id={event_id}&search_input={search_input}"
                
                headers = {
                    'accept': '*/*',
                    #'accept-encoding': 'gzip, deflate, br, zstd',
                    'accept-language': 'en-US,en;q=0.9',
                    'content-type': 'application/json',
                    'referer': f'https://racemateindia.com/leaderboard?id={event_id}',
                    'cookie': "connect.sid=s%3Af8IQAS8cy6for4Ai_T86BqtJ01C9Wr-_.%2FghuyjxH1ATLbFfyG0Xlofv0LZjjIBnj%2FDr%2BcidKKMw; _ga_CNYZKEMYX6=GS2.1.s1754458868$o1$g0$t1754458868$j60$l0$h0; _ga=GA1.2.899022246.1754458868; _gid=GA1.2.73190043.1754458868; _gat_gtag_UA_135512444_1=1; cf_clearance=J2uDf7LvlWX8L7E8OKVE1ZlYYbQNRh6Kr51OYTVMaKY-1754458869-1.2.1.1-W0Wqs6_D0d.IZf0X5OQlZlITaAgHUTuyzb3TCRgNdO2.NdM2pFL5mB8G0ED5_WcylAcMFpIgIB9fiOClqbhbekd2Te1r5OVOvR8VbDVeTDEUhBnvQu11fim_iQM32XQXd4tYlOapK5e_nZP0teXbO0oKMkxjdeA_4FLwpOqjICaO_HJWW_GKybdCc2qEcqqwTF3qE5HNAyRu4LEE5eeY_M6kXkAp1lgPZSzrcZk3iDg; _fbp=fb.1.1754458869880.82319836813206677",
                    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36 Edg/138.0.0.0',
                    'sec-ch-ua': '"Not)A;Brand";v="8", "Chromium";v="138", "Microsoft Edge";v="138"',
                    'sec-ch-ua-mobile': '?0',
                    'sec-ch-ua-platform': '"macOS"',
                    'sec-fetch-dest': 'empty',
                    'sec-fetch-mode': 'cors',
                    'sec-fetch-site': 'same-origin'
                }
                
                yield scrapy.Request(
                    url=search_url,
                    headers=headers,
                    callback=self.parse_search_results,
                    meta={
                        'event_name': self.event_name,
                        'year': self.year,
                        'event_id': event_id,
                        'bib': search_input
                    },
                    dont_filter=True
                )
        else:
            self.logger.error("Event name and year are required")
            return



    def parse_search_results(self, response):
        try:
        
            # Parse JSON response
            try:
                data = json.loads(response.body)
            except json.JSONDecodeError as e:
                self.logger.error(f"Invalid JSON response for search query: {response.meta['bib']}, Error: {str(e)}")
                return
            
            if data.get('status') != 1 or not data.get('data'):
                self.logger.info(f"No results found for search query: {response.meta['bib']}")
                return

            search_results = data.get('data', [])
            self.logger.info(f"Found {len(search_results)} results for search query: {response.meta['bib']}")
        

            # Process each search result
            for result in search_results:
                bib_no = result.get('bib_no')
                e_bib_no = result.get('e_bib_no')
                event_id = result.get('event_id')
                
                if not e_bib_no or not event_id:
                    continue

                # Create unique identifier to avoid duplicates
                unique_id = f"{event_id}_{e_bib_no}"
                if unique_id in self.processed_bibs:
                    self.logger.info(f"Skipping already processed result: {unique_id}")
                    continue

                self.processed_bibs.add(unique_id)
                
                # Construct the individual result URL
                result_url = f"https://racemateindia.com/individualResult?event_id={response.meta['event_id']}&bib_no={e_bib_no}"
                
                yield scrapy.Request(
                    url=result_url,
                    callback=self.parse_participant,
                    meta={
                        'event_name': response.meta['event_name'],
                        'year': response.meta['year'],
                        'event_id': response.meta['event_id'],
                        'search_result': result
                    },
                    dont_filter=True
                )

        except Exception as e:
            self.logger.error(f"Error in parse_search_results: {str(e)}")

    def parse_participant(self, response):
        try:
            self.logger.info(f"Processing participant page: {response.url}")
            
            # Extract personal information
            bib = response.xpath("//tr[contains(td, 'Bib No')]/td[2]/text()").get('').strip()
            runner_name = response.xpath("//tr[contains(td, 'Name')]/td[2]/text()").get('').strip()
            gender = response.xpath("//tr[contains(td, 'Gender')]/td[2]/text()").get('').strip()
            
            # Extract race information
            race_date = response.xpath("//div[contains(@class, 'EName')]/p/text()").get('').strip()
            race_name = response.xpath("//div[contains(@class, 'EName')]/h2/text()").get('').strip()
            distance_category = response.xpath("//tr[contains(td, 'Race')]/td[2]/text()").get('').strip()
            
            # Extract timing information
            net_time = response.xpath("//tr[contains(td, 'Net Time')]/td[2]/text()").get('').strip()
            gun_time = response.xpath("//tr[contains(td, 'Gun Time')]/td[2]/text()").get('').strip()
            
            # Extract ranking information
            rank_overall = response.xpath("//tr[contains(td, 'Overall Place')]/td[2]/text()").get('').strip()
            rank_gender = response.xpath("//tr[contains(td, 'Gender Place')]/td[2]/text()").get('').strip()
            age_category = response.xpath("//tr[contains(td, 'Age Category')]/td[2]/text()").get('').strip()
            rank_age_category = response.xpath("//tr[contains(td, 'Age Place')]/td[2]/text()").get('').strip()
            
            # Clean up ranking data to remove extra spaces around forward slashes
            if rank_overall:
                rank_overall = rank_overall.replace(' / ', '/')
            if rank_gender:
                rank_gender = rank_gender.replace(' / ', '/')
            if rank_age_category:
                rank_age_category = rank_age_category.replace(' / ', '/')
            
            # Extract pace information
            average_speed = response.xpath("//tr[contains(td, 'Average Speed')]/td[2]/text()").get('').strip()
            average_pace = response.xpath("//tr[contains(td, 'Average Pace')]/td[2]/text()").get('').strip()
            
            # Extract interval data
            interval_data = []
            interval_rows = response.xpath("//tr[td[1][contains(text(), '.000')]]")
            for row in interval_rows:
                interval_km = row.xpath("td[1]/text()").get('').strip()
                interval_time = row.xpath("td[2]/text()").get('').strip()
                interval_speed = row.xpath("td[3]/text()").get('').strip()
                interval_pace = row.xpath("td[4]/text()").get('').strip()
                
                if interval_km and interval_time:
                    interval_data.append({
                        'km': interval_km,
                        'time': interval_time,
                        'speed': interval_speed,
                        'pace': interval_pace
                    })

            yield {
                'summary': {
                    'event_id': response.meta['event_id'],
                    'race_name': response.meta['event_name'],
                    'race_date': race_date,
                    'master_event_id': response.meta['event_id']
                },
                'bib_number': bib,
                'distance_category': distance_category,
                'runner_name': runner_name,
                'gender': gender,
                'age_category': age_category,
                'finish_time_net': net_time,
                'finish_time_gun': gun_time,
                'chip_pace': '',
                'rank_overall': rank_overall,
                'rank_gender': rank_gender,
                'rank_age_category': rank_age_category,
                'jsonb': {
                    'bib': bib,
                    'nationality': '',
                    'age': '',
                    'race_category': distance_category,
                    'original_name': runner_name,
                    'overall_rank': rank_overall.split('/')[0] if rank_overall else '',
                    'gender': gender,
                    'net_time': net_time,
                    'gross_time': gun_time
                }
            }

        except Exception as e:
            self.logger.error(f"Error in parse_participant: {str(e)}") 