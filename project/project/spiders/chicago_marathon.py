import scrapy
import re
from urllib.parse import urljoin, urlparse, parse_qs
from datetime import datetime


class ChicagoMarathonSpider(scrapy.Spider):
    name = "chicago_marathon"
    
    custom_settings = {
        'AUTOTHROTTLE_ENABLED': False,
        'CONCURRENT_REQUESTS': 100,  # Increased for concurrent page requests
        'RETRY_ENABLED': True,
        'LOG_FILE': f"logs/chicago_marathon.log",
        'USER_AGENT': "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36 Edg/143.0.0.0",
        #'DOWNLOAD_DELAY': 0.5,  # Reduced delay for faster concurrent requests
        'COOKIES_ENABLED': True,
    }

    def __init__(self, *args, **kwargs):
        super(ChicagoMarathonSpider, self).__init__(*args, **kwargs)
        self.year = getattr(self, 'year', '2025')
        self.base_url = f"https://results.chicagomarathon.com/{self.year}/"
        self.max_page_extracted = False
        self.max_page = None

    def start_requests(self):
        """Start with POST request to search page"""
        search_url = f"{self.base_url}?pid=search"
        
        formdata = {
            'pid': 'search',
            'lang': 'EN_CAP',
            'startpage': 'start_responsive',
            'startpage_type': 'search',
            'event_main_group': 'runner',
            'event': 'MAR',
            'search[name]': '',
            'search[firstname]': '',
            'search[start_no]': '',
            'submit': '',
        }
        
        headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Encoding': 'gzip, deflate, br, zstd',
            'Accept-Language': 'en-US,en;q=0.9',
            'Content-Type': 'application/x-www-form-urlencoded',
            'Origin': 'https://results.chicagomarathon.com',
            'Referer': f'{self.base_url}?pid=search&pidp=start',
        }
        
        yield scrapy.FormRequest(
            url=search_url,
            formdata=formdata,
            headers=headers,
            callback=self.parse_search_results,
            meta={'year': self.year}
        )

    def parse_search_results(self, response):
        """Parse the search results page and extract runner detail links"""
        self.logger.info(f"Parsing search results: {response.url}")
        
        # Extract max page number from pagination on first page
        if not self.max_page_extracted:
            max_page = self.extract_max_page(response)
            if max_page:
                self.max_page = max_page
                self.max_page_extracted = True
                self.logger.info(f"Found maximum page number: {self.max_page}")
                
                # Generate all page URLs concurrently (starting from page 2, since we're already on page 1)
                for page_num in range(2, self.max_page + 1):
                    page_url = f"{self.base_url}?page={page_num}&event=MAR&event_main_group=runner&pid=search&pidp=start"
                    yield scrapy.Request(
                        url=page_url,
                        callback=self.parse_search_results,
                        headers={
                            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                            'Accept-Encoding': 'gzip, deflate, br, zstd',
                            'Accept-Language': 'en-US,en;q=0.9',
                            'Referer': f'{self.base_url}?pid=search&pidp=start',
                        },
                        meta={'year': self.year, 'page': page_num}
                    )
                # Continue to process runners on the first page
                # Set page number for first page
                if 'page' not in response.meta:
                    response.meta['page'] = 1
        
        # Find all runner list items (excluding the header)
        runner_items = response.css('ul.list-group.list-group-multicolumn li.list-group-item.row')
        
        page_num = response.meta.get('page', 1)
        self.logger.info(f"Found {len(runner_items)} runner entries on page {page_num}")
        
        for item in runner_items:
            # Skip header row
            item_classes = item.attrib.get('class', '')
            if 'list-group-header' in item_classes:
                continue
            
            # Extract detail link from the name field
            detail_link = item.css('h4.list-field.type-fullname a::attr(href)').get()
            
            if not detail_link:
                # Try alternative selector
                detail_link = item.css('a[href*="content=detail"]::attr(href)').get()
            
            if not detail_link:
                self.logger.warning(f"No detail link found for item")
                continue
            
            # Build full URL
            if detail_link.startswith('?'):
                detail_url = urljoin(self.base_url, detail_link)
            elif detail_link.startswith('http'):
                detail_url = detail_link
            else:
                detail_url = urljoin(self.base_url, detail_link)
            
            # Extract basic info from list item for early validation
            bib_text = item.css('div.list-field.type-field::text').get()
            if not bib_text:
                bib_text = item.xpath('.//div[contains(@class, "type-field")]//text()').get()
            bib_number = re.search(r'(\d+)', bib_text) if bib_text else None
            bib_number = bib_number.group(1) if bib_number else None
            
            # Extract places - handle both numeric and text-muted cases
            place_overall_elem = item.css('div.list-field.type-place.place-secondary')
            place_overall = None
            if place_overall_elem:
                place_text = place_overall_elem.css('::text').get()
                if place_text and '–' not in place_text:
                    place_match = re.search(r'(\d+)', place_text)
                    place_overall = place_match.group(1) if place_match else None
            
            place_gender_elem = item.css('div.list-field.type-place.place-primary')
            place_gender = None
            if place_gender_elem:
                place_text = place_gender_elem.css('::text').get()
                if place_text and '–' not in place_text:
                    place_match = re.search(r'(\d+)', place_text)
                    place_gender = place_match.group(1) if place_match else None
            
            name_text = item.css('h4.list-field.type-fullname a::text').get()
            
            self.logger.info(f"Found runner: {name_text}, Bib: {bib_number}, URL: {detail_url}")
            
            # Request detail page
            yield scrapy.Request(
                url=detail_url,
                callback=self.parse_runner_detail,
                headers={
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                    'Accept-Encoding': 'gzip, deflate, br, zstd',
                    'Accept-Language': 'en-US,en;q=0.9',
                    'Referer': f'{self.base_url}?pid=search&pidp=start',
                    'Sec-Fetch-Dest': 'document',
                    'Sec-Fetch-Mode': 'navigate',
                    'Sec-Fetch-Site': 'same-origin',
                    'Sec-Fetch-User': '?1',
                    'Upgrade-Insecure-Requests': '1',
                },
                meta={
                    'year': self.year,
                    'bib_number': bib_number,
                    'place_overall': place_overall,
                    'place_gender': place_gender,
                }
            )

    def extract_max_page(self, response):
        """Extract the maximum page number from pagination"""
        try:
            # Look for pagination links
            pagination_links = response.css('ul.pagination li a[href*="page="]::attr(href)').getall()
            
            max_page = 1
            for link in pagination_links:
                # Extract page number from href
                page_match = re.search(r'page=(\d+)', link)
                if page_match:
                    page_num = int(page_match.group(1))
                    if page_num > max_page:
                        max_page = page_num
            
            # Also try to get page numbers from link text
            page_texts = response.css('ul.pagination li a::text').getall()
            for text in page_texts:
                if text and text.strip().isdigit():
                    page_num = int(text.strip())
                    if page_num > max_page:
                        max_page = page_num
            
            # Alternative: look for the last visible page number in pagination
            last_page_link = response.css('ul.pagination li:last-child a[href*="page="]::attr(href)').get()
            if last_page_link:
                page_match = re.search(r'page=(\d+)', last_page_link)
                if page_match:
                    page_num = int(page_match.group(1))
                    if page_num > max_page:
                        max_page = page_num
            
            return max_page if max_page > 1 else None
            
        except Exception as e:
            self.logger.error(f"Error extracting max page: {str(e)}")
            return None

    def parse_runner_detail(self, response):
        """Parse detailed runner information from detail page"""
        self.logger.info(f"Parsing runner detail: {response.url}")
        
        try:
            # Extract name with citizenship
            name_ctz = response.css('tr.f-__fullname td.last::text').get()
            if not name_ctz:
                name_ctz = response.css('tr.f-__fullname td::text').get()
            if not name_ctz:
                name_ctz = response.css('td.f-__fullname::text').get()
            name_ctz = name_ctz.strip() if name_ctz else ''
            
            # Extract name and nationality
            name_match = re.match(r'(.+?)\s*\(([A-Z]{3})\)', name_ctz)
            if name_match:
                runner_name = name_match.group(1).strip()
                nationality = name_match.group(2)
            else:
                runner_name = name_ctz
                nationality = ''
            
            # Extract bib number
            bib_number = response.meta.get('bib_number')
            if not bib_number:
                bib_number = response.css('tr.f-start_no_text td.last::text').get()
            if not bib_number:
                bib_number = response.css('tr.f-start_no_text td::text').get()
            if bib_number:
                bib_number = bib_number.strip()
            else:
                bib_number = ''
            
            # Extract city, state
            city_state = response.css('tr.f-__city_state td.last::text').get()
            if not city_state:
                city_state = response.css('tr.f-__city_state td::text').get()
            city_state = city_state.strip() if city_state else ''
            
            # Extract division (age group)
            division = response.css('tr.f-_type_age_class td.last::text').get()
            if not division:
                division = response.css('tr.f-_type_age_class td::text').get()
            division = division.strip() if division else ''
            
            # Extract gender
            gender_text = response.css('tr.f-_type_sex td.last::text').get()
            if not gender_text:
                gender_text = response.css('tr.f-_type_sex td::text').get()
            gender_text = gender_text.strip() if gender_text else ''
            gender = 'Male' if 'Man' in gender_text or 'M' in gender_text else 'Female'
            
            # Extract rankings
            place_gender = response.css('tr.f-place_all td.last::text').get()
            if not place_gender:
                place_gender = response.css('tr.f-place_all td::text').get()
            place_gender = place_gender.strip() if place_gender else response.meta.get('place_gender', '')
            
            place_age = response.css('tr.f-place_age td.last::text').get()
            if not place_age:
                place_age = response.css('tr.f-place_age td::text').get()
            place_age = place_age.strip() if place_age else ''
            
            place_overall = response.css('tr.f-place_nosex td.last::text').get()
            if not place_overall:
                place_overall = response.css('tr.f-place_nosex td::text').get()
            place_overall = place_overall.strip() if place_overall else response.meta.get('place_overall', '')
            
            # Extract finish time
            finish_time = response.css('tr.f-time_finish_netto td.last::text').get()
            if not finish_time:
                finish_time = response.css('tr.f-time_finish_netto td::text').get()
            finish_time = finish_time.strip() if finish_time else ''
            
            # Extract start time
            start_time = response.css('tr.f-starttime_net td.last::text').get()
            if not start_time:
                start_time = response.css('tr.f-starttime_net td::text').get()
            start_time = start_time.strip() if start_time else ''
            
            # Extract splits
            splits = {}
            split_rows = response.css('div.box-splits table tbody tr')
            for row in split_rows:
                split_name = row.css('th.desc::text').get()
                split_time = row.css('td.time::text').get()
                if split_name and split_time:
                    splits[split_name.strip()] = split_time.strip()
            
            # Extract race status
            race_status = response.css('tr.f-__race_status td::text').get()
            race_status = race_status.strip() if race_status else ''
            
            # Format rankings - match sportstimingsolutions format
            rank_overall = place_overall if place_overall else ''
            rank_gender = place_gender if place_gender else ''
            rank_age_category = place_age if place_age else ''
            
            # Extract age from division if possible
            age = ''
            if division:
                age_match = re.search(r'(\d+)', division)
                if age_match:
                    age = age_match.group(1)
            
            # Build result in standard format matching sportstimingsolutions.py
            result = {
                'summary': {
                    'event_id': f'chicago_marathon_{response.meta["year"]}',
                    'race_name': f'Bank of America Chicago Marathon {response.meta["year"]}',
                    'race_date': f'{response.meta["year"]}-10-13',  # Chicago Marathon is typically in October
                    'master_event_id': f'chicago_marathon_{response.meta["year"]}',
                },
                'bib_number': bib_number,
                'distance_category': 'Marathon',
                'runner_name': runner_name,
                'gender': gender,
                'age_category': division,  # Match sportstimingsolutions format (empty string)
                'finish_time_net': finish_time,
                'finish_time_gun': '',  # Not available in detail page
                'chip_pace': '',  # Can be calculated if needed
                'rank_overall': rank_overall or '',
                'rank_gender': rank_gender or '',
                'rank_age_category': rank_age_category or '',
                'jsonb': {
                    'bib': bib_number,
                    'nationality': nationality,
                    'age': '',  # Match sportstimingsolutions format (empty string)
                    'race_category': 'Marathon',
                    'original_name': name_ctz,
                    'overall_rank': rank_overall.split('/')[0] if rank_overall else '',  # Extract just the number part
                    'gender': gender,
                    'net_time': finish_time,
                    'gross_time': ''  # Not available
                }
            }
            
            yield result
            
        except Exception as e:
            self.logger.error(f"Error parsing runner detail {response.url}: {str(e)}")
            return

