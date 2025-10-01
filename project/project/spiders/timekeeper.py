import scrapy
from scrapy import Request, FormRequest
from urllib.parse import urljoin
import json
from datetime import datetime

class TimekeeperSpider(scrapy.Spider):
    name = 'timekeeper'
    
    custom_settings = {
        'ROBOTSTXT_OBEY': False,
        'CONCURRENT_REQUESTS': 50,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 50,
        #'DOWNLOAD_DELAY': 0.1,
        'COOKIES_ENABLED': True,
        'DEFAULT_REQUEST_HEADERS': {
            'Accept': 'text/html, */*; q=0.01',
            'Accept-Encoding': 'gzip, deflate, br, zstd',
            'Accept-Language': 'en-US,en;q=0.9',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36 Edg/136.0.0.0',
            'sec-ch-ua': '"Chromium";v="136", "Microsoft Edge";v="136", "Not.A/Brand";v="99"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"macOS"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'x-requested-with': 'XMLHttpRequest'
        }
    }

    def __init__(self, *args, **kwargs):
        super(TimekeeperSpider, self).__init__(*args, **kwargs)
        self.base_url = 'https://timekeeper.co.in'
        self.search_url = f'{self.base_url}/web/result/search_result/2024/JBG%20SATARA%20HILL%20HALF%20MARATHON'
        self.api_url = f'{self.base_url}/web/result/get_leaderboard_result'
        self.race_date = '2024-09-01'  # JBG SATARA HILL HALF MARATHON date
        self.start_bib = 1
        self.end_bib = 99999

    def start_requests(self):
        # First make a GET request to the search page to get cookies
        yield Request(
            url=self.search_url,
            callback=self.parse_search_page,
            headers={'Referer': self.base_url}
        )

    def parse_search_page(self, response):
        # After getting cookies, make POST requests for each BIB number
        for bib in range(self.start_bib, self.end_bib + 1):
            formdata = {
                'event': '43',
                'race': '',
                'racecategory': '',
                'bib': str(bib),
                'participant_type': '',
                'gender': '',
                'pty': ''
            }
            
            yield FormRequest(
                url=self.api_url,
                method='POST',
                formdata=formdata,
                callback=self.parse_results,
                headers={
                    'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
                    'Origin': self.base_url,
                    'Referer': self.search_url
                },
                meta={'bib': str(bib)}
            )

    def parse_results(self, response):
        try:
            # Check if we got valid data (no results will return empty HTML)
            if not response.xpath('//h4[@class="name"]/text()').get():
                return

            # Extract basic info
            name = response.xpath('//h4[@class="name"]/text()').get('').strip()
            bib = response.meta['bib']  # Use the BIB from meta
            
            # Extract rankings with total ranks
            # Overall rank
            rank_overall = response.xpath('//div[@class="row row-cols-1 row-cols-md-3 row-cols-lg-3 verticall-middle"]/div[@class="col-md-4"][1]/div/h1[1]/text()').get('').strip()
            total_rank_overall = response.xpath('//div[@class="row row-cols-1 row-cols-md-3 row-cols-lg-3 verticall-middle"]/div[@class="col-md-4"][1]/div/h1[3]/text()').get('').strip()
            
            # Gender rank
            rank_gender = response.xpath('//div[@class="row row-cols-1 row-cols-md-3 row-cols-lg-3 verticall-middle"]/div[@class="col-md-4"][2]/div/h1[1]/text()').get('').strip()
            total_rank_gender = response.xpath('//div[@class="row row-cols-1 row-cols-md-3 row-cols-lg-3 verticall-middle"]/div[@class="col-md-4"][2]/div/h1[3]/text()').get('').strip()
            
            # Category rank
            rank_category = response.xpath('//div[@class="row row-cols-1 row-cols-md-3 row-cols-lg-3 verticall-middle"]/div[@class="col-md-4"][3]/div/h1[1]/text()').get('').strip()
            total_rank_category = response.xpath('//div[@class="row row-cols-1 row-cols-md-3 row-cols-lg-3 verticall-middle"]/div[@class="col-md-4"][3]/div/h1[3]/text()').get('').strip()
            
            # Format rankings as "rank/total_rank"
            rank_overall = f"{rank_overall}/{total_rank_overall}" if rank_overall and total_rank_overall else ''
            rank_gender = f"{rank_gender}/{total_rank_gender}" if rank_gender and total_rank_gender else ''
            rank_category = f"{rank_category}/{total_rank_category}" if rank_category and total_rank_category else ''
            
            # Extract timing info
            finish_time = response.xpath('//div[contains(@class, "timing-block")]//h1[contains(@class, "time-head1-v")]/text()').get('').strip()
            chip_pace = response.xpath('//div[contains(@class, "timing-block")]//h1[contains(@class, "time-head2-v")]/text()').get('').strip()
            
            # Extract category
            category = response.xpath('//h6[contains(@class, "rc-srch-text") and contains(., "Category")]/text()').get('').strip()
            if category:
                category = category.split(':')[1].strip()
            
            # Extract gender from category
            gender = ''
            if 'Male' in category:
                gender = 'M'
            elif 'Female' in category:
                gender = 'F'
            
            # Create result structure
            result = {
                'summary': {
                    'event_id': 'JBG2024',
                    'race_name': 'JBG SATARA HILL HALF MARATHON',
                    'race_date': self.race_date,
                    'master_event_id': 'JBG2024'
                },
                'bib_number': bib,
                'distance_category': 'Half Marathon',
                'runner_name': name,
                'gender': gender,
                'age_category': category,
                'finish_time_net': finish_time,
                'finish_time_gun': '',
                'chip_pace': chip_pace,
                'rank_overall': rank_overall,
                'rank_gender': rank_gender,
                'rank_age_category': rank_category,
                'jsonb': {
                    'bib': bib,
                    'nationality': '',
                    'age': '',
                    'race_category': 'Half Marathon',
                    'original_name': name,
                    'overall_rank': rank_overall,
                    'gender': gender,
                    'net_time': finish_time,
                    'gross_time': ''
                }
            }

            yield result
        except Exception as e:
            self.logger.error(f"Error processing results for BIB {response.meta.get('bib', 'unknown')}: {str(e)}")
            return 