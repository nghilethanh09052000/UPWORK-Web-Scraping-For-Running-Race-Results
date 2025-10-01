import scrapy
import json
import logging
from datetime import datetime
from urllib.parse import urljoin

class RunizenSpider(scrapy.Spider):
    name = 'runizen'
    custom_settings = {
        'AUTOTHROTTLE_ENABLED': False,
        'CONCURRENT_REQUESTS': 200,
        # 'CONCURRENT_REQUESTS_PER_DOMAIN': 50,
        # 'CONCURRENT_REQUESTS_PER_IP': 50,
        'DOWNLOADER_MIDDLEWARES': {
            'project.middlewares.CustomProxyMiddleware': 543,
        },
        'RETRY_ENABLED': True,
        'LOG_FILE': f"logs/runizen.log",
        'USER_AGENT': "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36 Edg/136.0.0.0"
    }

    def __init__(self, *args, **kwargs):
        super(RunizenSpider, self).__init__(*args, **kwargs)
        self.processed_bibs = set()
        self.events = {
            'Vedanta Pink City Half': {
                '2022': 'KERF1P',
                '2023': 'KVgFwQ',
                '2024': 'K24Fpe'
            },
            'Vedanta Zinc City Half': {
                '2024': 'r3nF2Z'
            },
            # New events added
            'Sonipat Half Marathon': {
                '2025': 'rj2FA2'
            }
        }
        self.current_event = None
        self.current_year = None
        # Get bib range parameters
        self.start_bib = int(getattr(self, 'start_bib', 1))
        self.end_bib = int(getattr(self, 'end_bib', 99999))

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
                
            yield self.make_event_request(self.event_name, self.year, event_id)
        else:
            self.logger.error("Event name and year are required")
            return

    def make_event_request(self, event_name, year, event_id):

        search_url = f"https://runizen.com/e/{event_id}/search?search_unpublished=false"
        headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            #'accept-encoding': 'gzip, deflate, br, zstd',
            'accept-language': 'en-US,en;q=0.9',
            #'content-type': 'application/x-www-form-urlencoded;charset=UTF-8',
            'origin': 'https://runizen.com',
            'referer': f'https://runizen.com/e/{event_id}',
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36 Edg/136.0.0.0',
            'x-csrf-token': '',  
            'x-turbo-request-id': ''
        }

        self.logger.info(f"Searching for event: {event_name} {year}")
        
        # First get the initial page to get CSRF token
        return scrapy.Request(
            url=f'https://runizen.com/e/{event_id}',
            headers=headers,
            callback=self.parse_initial_page,
            meta={
                'event_name': event_name,
                'year': year,
                'event_id': event_id,
                'search_url': search_url
            },
            dont_filter=True
        )
 

    def parse_initial_page(self, response):
        # Extract CSRF token from the page
        csrf_token = response.xpath('//meta[@name="csrf-token"]/@content').get()
        if not csrf_token:
            self.logger.error("Could not find CSRF token")
            return
        
        authenticity_token = response.xpath('//input[@name="authenticity_token"]/@value').get()

        # Update headers with CSRF token
        headers = {
            "accept": "text/vnd.turbo-stream.html, text/html, application/xhtml+xml",
            "accept-encoding": "gzip, deflate, br, zstd",
            "accept-language": "en-US,en;q=0.9",
            "content-length": "114",
            "content-type": "application/x-www-form-urlencoded;charset=UTF-8",
            "cookie": "_runizen_session=EkJeGcTqtuHz79wO9KpczhTdncIrU5Z2D06X37IlKvtW0mregl%2FUYJ9AAAs%2FaWAPdbAjCfQypcA5zLMdmb3I%2B7U0NoeN7Y5uW%2FHls%2FSDte3DFEKAXkAZcApLTF%2BzCUYSXSI4KbL0vkyuKGLfh0LZ1Pfbq5ikwsJWJpzLanxoq6nUxPCJETXpe0LHXVnVCn27CZCKx2Afyzkm5el7bjvtrmUJmPBgnWWjAOe2hGXtTJHbnZBdBZ1Np15AhcFEfibAGHZq%2BLW30JTW%2B05itV5DjZQlNDTjiU9B--rmH%2F5uosEvlYc8Y2--7TXfjTjmQ%2FaFISoiQaK8kQ%3D%3D",
            "newrelic": "eyJ2IjpbMCwxXSwiZCI6eyJ0eSI6IkJyb3dzZXIiLCJhYyI6IjI4MzkyMjUiLCJhcCI6IjExMjAyOTE4NTYiLCJpZCI6ImM4OGFhNzliOTQ3ZDk2NzciLCJ0ciI6IjZhODdkM2IxYjZlNDc4ZWNkNjY5YTEyMjdmY2Q0NjJiIiwidGkiOjE3NTQ0NDczMzYxNDB9fQ==",
            "origin": "https://runizen.com",
            "priority": "u=1, i",
            "referer": "https://runizen.com/e/rj2FA2",
            "sec-ch-ua": "\"Not)A;Brand\";v=\"8\", \"Chromium\";v=\"138\", \"Microsoft Edge\";v=\"138\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"macOS\"",
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "traceparent": "00-6a87d3b1b6e478ecd669a1227fcd462b-c88aa79b947d9677-01",
            "tracestate": "2839225@nr=0-1-2839225-1120291856-c88aa79b947d9677----1754447336140",
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36 Edg/138.0.0.0",
            #"x-csrf-token": "3bFX3yKgf-0RFBBg2pLWKOEwEB55k5zRRC4ym5Nc0ygUuEtpMeuV0a5mrkbAXCnzcU3QzKPWgVzVVEBXHuDotA",
            "x-turbo-request-id": "2a74b3bb-8051-4c0b-b1bd-69d947634a7a"
            }

        headers['x-csrf-token'] = csrf_token

        # Start searching for bibs
        for bib in range(self.start_bib, self.end_bib + 1):
            formdata = {
                'authenticity_token': csrf_token,
                'query': str(bib)
            }
            
            yield scrapy.FormRequest(
                url=response.meta['search_url'],
                method='POST',
                formdata=formdata,
                headers=headers,
                callback=self.parse_search_results,
                meta={
                    'event_name': response.meta['event_name'],
                    'year': response.meta['year'],
                    'event_id': response.meta['event_id'],
                    'bib': str(bib)
                },
                dont_filter=True
            )

    def parse_search_results(self, response):
        try:
            # Extract participant links and bib numbers from the search results
            participant_items = response.xpath('//ul/li/a')
            
            if not participant_items:
                self.logger.info(f"No results found for search query: {response.meta['bib']}")
                return

            self.logger.info(f"Found {len(participant_items)} results for search query: {response.meta['bib']}")

            # Process each participant link
            for item in participant_items:
                link = item.xpath('@href').get()
                if link in self.processed_bibs:
                    self.logger.info(f"Skipping already processed link: {link}")
                    continue

                # Extract the full bib number from the text content
                text_content = item.xpath('text()').get('').strip()
                # Extract the bib number from the text (it's between parentheses)
                bib_match = text_content.split('(')[-1].split(')')[0].strip()
                if not bib_match:
                    continue

                self.processed_bibs.add(link)
                full_url = urljoin('https://runizen.com', link)
                
                yield scrapy.Request(
                    url=full_url,
                    callback=self.parse_participant,
                    meta={
                        'event_name': response.meta['event_name'],
                        'year': response.meta['year'],
                        'event_id': response.meta['event_id']
                    },
                    dont_filter=True
                )

        except Exception as e:
            self.logger.error(f"Error in parse_search_results: {str(e)}")

    def parse_participant(self, response):
        try:
            self.logger.info(f"Processing participant page: {response.url}")
            
            # Extract personal information
            bib = response.xpath("//div[@class='rounded bg-sky-500 text-white m-4 p-4 w-48 shadow min-h-[100px] flex flex-col'][1]/div[2]/text()").get('').strip()
            race_date = response.xpath("//div[@class='md:text-lg text-base text-center text-gray-600']/text()").get('').strip()
            runner_name = response.xpath("//div[@class='text-xl font-semibold text-gray-800']/text()").get('').strip()
            distance_category = response.xpath("//div[@class='rounded bg-sky-500 text-white m-4 p-4 w-48 shadow min-h-[100px] flex flex-col'][4]/div[2]/text()").get('').strip()
            gender = response.xpath("//div[@class='rounded bg-sky-500 text-white m-4 p-4 w-48 shadow min-h-[100px] flex flex-col'][2]/div[2]/text()").get('').strip()
            age_category = response.xpath("//div[@class='rounded bg-sky-500 text-white m-4 p-4 w-48 shadow min-h-[100px] flex flex-col'][3]/div[2]/text()").get('').strip()

            # Extract chip time (net time)
            chip_time = response.xpath("//div[contains(text(), 'Timing (Chip time)')]/following-sibling::div//div[contains(@class, 'is-size-1')]/text()").get('').strip()
            chip_pace = response.xpath("//div[contains(text(), 'Timing (Chip time)')]/following-sibling::div//div[contains(@class, 'is-size-5')]/text()").get('').strip()
            if chip_pace:
                chip_pace = chip_pace.replace('Average Pace:', '').strip()
            
            # Extract gun time
            gun_time = response.xpath("//div[contains(text(), 'Timing (Gun time)')]/following-sibling::div//div[contains(@class, 'is-size-1')]/text()").get('').strip()
            gun_pace = response.xpath("//div[contains(text(), 'Timing (Gun time)')]/following-sibling::div//div[contains(@class, 'is-size-5')]/text()").get('').strip()
            if gun_pace:
                gun_pace = gun_pace.replace('Average Pace:', '').strip()
            
            # Extract chip time rankings
            chip_rank_overall = response.xpath("//div[contains(text(), 'Timing (Chip time)')]/following-sibling::div//div[contains(@class, 'bg-teal-400')]/following-sibling::div[contains(@class, 'is-size-4')]/text()").get('').strip()
            chip_total_rank_overall = response.xpath("//div[contains(text(), 'Timing (Chip time)')]/following-sibling::div//div[contains(@class, 'bg-teal-400')]/following-sibling::div[contains(@class, 'is-size-4')]/div[contains(@class, 'is-size-6')][1]/text()").get('').strip()
            if chip_total_rank_overall:
                chip_total_rank_overall = chip_total_rank_overall.replace('of', '').strip()
            rank_overall = f"{chip_rank_overall}/{chip_total_rank_overall}" if chip_rank_overall and chip_total_rank_overall else ''
            
            chip_rank_gender = response.xpath("//div[contains(text(), 'Timing (Chip time)')]/following-sibling::div//div[contains(@class, 'bg-red-600')]/following-sibling::div[contains(@class, 'is-size-4')]/text()").get('').strip()
            chip_total_rank_gender = response.xpath("//div[contains(text(), 'Timing (Chip time)')]/following-sibling::div//div[contains(@class, 'bg-red-600')]/following-sibling::div[contains(@class, 'is-size-4')]/div[contains(@class, 'is-size-6')][1]/text()").get('').strip()
            if chip_total_rank_gender:
                chip_total_rank_gender = chip_total_rank_gender.replace('of', '').strip()
            rank_gender = f"{chip_rank_gender}/{chip_total_rank_gender}" if chip_rank_gender and chip_total_rank_gender else ''
            
            chip_rank_category = response.xpath("//div[contains(text(), 'Timing (Chip time)')]/following-sibling::div//div[contains(@class, 'bg-sky-600')]/following-sibling::div[contains(@class, 'is-size-4')]/text()").get('').strip()
            chip_total_rank_category = response.xpath("//div[contains(text(), 'Timing (Chip time)')]/following-sibling::div//div[contains(@class, 'bg-sky-600')]/following-sibling::div[contains(@class, 'is-size-4')]/div[contains(@class, 'is-size-6')][1]/text()").get('').strip()
            if chip_total_rank_category:
                chip_total_rank_category = chip_total_rank_category.replace('of', '').strip()
            rank_category = f"{chip_rank_category}/{chip_total_rank_category}" if chip_rank_category and chip_total_rank_category else ''

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
                'finish_time_net': chip_time,
                'finish_time_gun': gun_time,
                'chip_pace': chip_pace,
                'rank_overall': rank_overall,
                'rank_gender': rank_gender,
                'rank_age_category': rank_category,
                'jsonb': {
                    'bib': bib,
                    'nationality': '',
                    'age': '',
                    'race_category': distance_category,
                    'original_name': runner_name,
                    'overall_rank': rank_overall.split('/')[0] if rank_overall else '',
                    'gender': gender,
                    'net_time': chip_time,
                    'gross_time': gun_time
                }
            }

        except Exception as e:
            self.logger.error(f"Error in parse_participant: {str(e)}") 