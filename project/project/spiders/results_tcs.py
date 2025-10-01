import scrapy
from scrapy import Request
from urllib.parse import urljoin
import json
from datetime import datetime

class ResultsTCSSpider(scrapy.Spider):
    name = 'results_tcs'
    
    custom_settings = {
        'ROBOTSTXT_OBEY': False,
        'CONCURRENT_REQUESTS': 50,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 50,
        'DOWNLOAD_DELAY': 0.1,  # Small delay to avoid overwhelming the server
        'COOKIES_ENABLED': True,
        'DEFAULT_REQUEST_HEADERS': {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Encoding': 'gzip, deflate, br, zstd',
            'Accept-Language': 'en-US,en;q=0.9',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36 Edg/136.0.0.0',
            'sec-ch-ua': '"Chromium";v="136", "Microsoft Edge";v="136", "Not.A/Brand";v="99"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"macOS"',
            'sec-fetch-dest': 'document',
            'sec-fetch-mode': 'navigate',
            'sec-fetch-site': 'same-origin',
            'sec-fetch-user': '?1',
            'upgrade-insecure-requests': '1'
        }
    }

    def __init__(self, *args, **kwargs):
        super(ResultsTCSSpider, self).__init__(*args, **kwargs)
        self.base_url = 'https://results.tcslondonmarathon.com/2025/'
        self.start_url = f'{self.base_url}?page=1&event=ALL&pid=search&pidp=start'
        self.race_date = '2025-04-27'  # TCS London Marathon 2025 date

    def start_requests(self):
        # First request to get the total number of pages
        yield Request(
            url=self.start_url,
            callback=self.parse_pagination,
            headers={
                'Referer': self.base_url
            }
        )

    def parse_pagination(self, response):
        # Extract all page numbers from pagination
        page_links = response.xpath('//ul[contains(@class, "pagination")]/li[contains(@class, "hidden-xs")]/a/@href').getall()
        
        # Extract page numbers from URLs
        page_numbers = []
        for link in page_links:
            try:
                page_num = int(link.split('page=')[1].split('&')[0])
                page_numbers.append(page_num)
            except:
                continue
        
        # Get the last page number
        last_page = max(page_numbers) if page_numbers else 1
        
        # Generate URLs for all pages
        for page in range(1, last_page + 1):
            page_url = f'{self.base_url}?page={page}&event=ALL&pid=search&pidp=start'
            yield Request(
                url=page_url,
                callback=self.parse_results_list,
                headers={'Referer': self.base_url},
                meta={'page': page}
            )

    def parse_results_list(self, response):
        # Extract runner entries from the results list
        runner_entries = response.xpath('//li[contains(@class, "list-group-item") and not(contains(@class, "list-group-header"))]')
        
        for entry in runner_entries:
            try:
                # Extract basic info from the list view
                name = entry.xpath('.//h4[@class=" list-field type-fullname"]/a/text()').get('').strip()
                
               
                
                bib = entry.xpath('.//div[contains(@class, "type-field") and contains(., "Runner Number")]/text()').get('').strip()
                category = entry.xpath('.//div[contains(@class, "type-age_class")]/text()').get('').strip()
                event = entry.xpath('.//div[contains(@class, "type-event_name")]/text()').get('').strip()
                finish_time = entry.xpath('.//div[@class="col-xs-12 col-sm-12 col-md-7 list-field-wrap"]//div[@class="pull-right"]//div[@class="split list-field type-time"]/text()').get('').strip()
                

                if event not in ['Elite', 'Mass']: 
                    print(f'Name is {name}.. continue')
                    continue
                
                # Extract nationality from name (format: "Name (COUNTRY)")
                nationality = ''
                if name and '(' in name:
                    try:
                        nationality = name.split('(')[-1].strip(')')
                        name = name.split('(')[0].strip()
                    except:
                        nationality = ''
                
                # Create initial result structure
                result = {
                    'summary': {
                        'event_id': 'TCS2025',  # Fixed event ID for TCS London Marathon 2025
                        'race_name': 'TCS London Marathon',
                        'race_date': self.race_date,
                        'master_event_id': 'TCS2025'
                    },
                    'bib_number': bib,
                    'distance_category': event,
                    'runner_name': name,
                    'gender': '',  # Will be updated from detail page
                    'age_category': category,
                    'finish_time_net': finish_time,
                    'finish_time_gun': '',  # Will be updated from detail page
                    'chip_pace': '',  # Will be updated from detail page
                    'rank_overall': '',  # Will be updated from detail page
                    'rank_gender': '',  # Will be updated from detail page
                    'rank_age_category': '',  # Will be updated from detail page
                    'jsonb': {
                        'bib': bib,
                        'nationality': nationality,
                        'age': '',  # Will be extracted from category if possible
                        'race_category': event,
                        'original_name': name,
                        'overall_rank': '',  # Will be updated from detail page
                        'gender': '',  # Will be updated from detail page
                        'net_time': finish_time,
                        'gross_time': ''  # Will be updated from detail page
                    }
                }

                # Get the detail page URL
                detail_url = entry.xpath('.//h4[@class=" list-field type-fullname"]/a/@href').get()
                if detail_url:
                    full_detail_url = urljoin(self.base_url, detail_url)
                    yield Request(
                        url=full_detail_url,
                        callback=self.parse_runner_detail,
                        meta={'result': result},
                        headers={'Referer': response.url}
                    )
            except Exception as e:
                self.logger.error(f"Error processing entry: {str(e)}")
                continue

    def parse_runner_detail(self, response):
        try:
            result = response.meta['result']
            
            # Extract ranking information
            rank_category = response.xpath('//tr[contains(@class, "f-place_age")]/td[1]/text()').get('')
            rank_gender = response.xpath('//tr[contains(@class, "f-place_all")]/td[1]/text()').get('')
            rank_overall = response.xpath('//tr[contains(@class, "f-place_nosex")]/td[1]/text()').get('')
            
            # Update rankings in the format {rank}/{total_rank}
            result['rank_overall'] = f"{rank_overall}" if rank_overall else ''
            result['rank_gender'] = f"{rank_gender}" if rank_gender  else ''
            result['rank_age_category'] = f"{rank_category}" if rank_category  else ''
            
            # Update overall_rank in jsonb to only include the rank number (not the total)
            result['jsonb']['overall_rank'] = rank_overall if rank_overall else ''
            
            # Determine gender from ranking information
            if rank_gender:
                result['gender'] = 'M' if int(rank_gender) > 0 else 'F'
                result['jsonb']['gender'] = result['gender']
            
            # Extract chip pace from the finish split
            chip_pace = response.xpath('//div[contains(@class, "box-splits")]//tr[last()]//td[@class="min_km"]/text()').get('')
            if chip_pace:
                result['chip_pace'] = chip_pace
                result['jsonb']['chip_pace'] = chip_pace
            
            yield result
        except Exception as e:
            self.logger.error(f"Error processing detail page: {str(e)}")
            yield response.meta['result']  # Yield the original result if detail page processing fails 