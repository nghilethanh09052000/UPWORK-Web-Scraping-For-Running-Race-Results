import scrapy
import json
import logging
import re
from datetime import datetime
from urllib.parse import urljoin

class MultiSportAustraliaSpider(scrapy.Spider):
    name = 'multisportaustralia'
    custom_settings = {
        
        'AUTOTHROTTLE_ENABLED': False,             # Disable throttling
        'DOWNLOAD_DELAY': 0,                       # No delay between requests
        'CONCURRENT_REQUESTS': 512,                # Total concurrent requests (aggressive)
        'CONCURRENT_REQUESTS_PER_DOMAIN': 256,     # Adjust depending on proxies
        'CONCURRENT_REQUESTS_PER_IP': 256,         # Assumes many proxies/IPs
            
        'RETRY_ENABLED': True,
        'DOWNLOADER_MIDDLEWARES': {
            'project.middlewares.CustomProxyMiddleware': 543,
        },
        'LOG_FILE': f"logs/multisportaustralia.log",
        'USER_AGENT': "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36 Edg/136.0.0.0"
    }

    def __init__(self, *args, **kwargs):
        super(MultiSportAustraliaSpider, self).__init__(*args, **kwargs)
        self.processed_individuals = set()
        self.events = {
            'sydney-marathon-2024': '2024',
            'sydney-marathon-2023': '2023', 
            'blackmores-sydney-running-festival-2022': '2022'
        }
        self.current_event = None
        self.current_year = None

    def start_requests(self):
        self.event_name = getattr(self, 'event_name', None)
        self.year = getattr(self, 'year', None)
        
        if self.event_name and self.year:
            self.logger.info(f"Starting spider for event: {self.event_name}, year: {self.year}")
            yield self.make_event_request(self.event_name, self.year)
        else:
            self.logger.error("Event name and year are required")
            return

    def make_event_request(self, event_name, year):
        base_url = f"https://www.multisportaustralia.com.au/races/{event_name}/"
        headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36 Edg/136.0.0.0',
            'referer': 'https://www.multisportaustralia.com.au/'
        }

        self.logger.info(f"Fetching event page: {base_url}")
        
        return scrapy.Request(
            url=base_url,
            headers=headers,
            callback=self.parse_event_page,
            meta={
                'event_name': event_name,
                'year': year
            },
            dont_filter=True
        )

    def parse_event_page(self, response):
        try:
            event_name = response.meta['event_name']
            year = response.meta['year']
            
            self.logger.info(f"Parsing event page for {event_name} {year}")
            
            # Extract all text-white links (including those with text-decoration-none)
            event_links = response.xpath('//a[contains(@class, "text-white")]')
            
            # Track unique hrefs to avoid duplicates
            seen_hrefs = set()
            
            for link in event_links:
                href = link.xpath('@href').get()
                link_text = link.xpath('text()').get()
                
                if 'leaderboard' in href:
                    self.logger.info(f"Skipping leaderboard/results link: {href}")
                    continue
                
                # Skip if we've already seen this href
                if href in seen_hrefs:
                    continue
                
                seen_hrefs.add(href)
                
                if not href or not link_text:
                    continue
                
                # Check if this is a valid event (exclude virtual and wheelchair)
                link_text_lower = link_text.lower()
                if 'virtual' in link_text_lower or 'wheelchair' in link_text_lower:
                    self.logger.info(f"Skipping virtual/wheelchair event: {link_text}")
                    continue
                
                self.logger.info(f"Found valid event: {link_text} - {href}")
                
                # Make the href absolute URL
                event_url = urljoin(response.url, href)
                
                # Remove trailing slash if present
                if event_url.endswith('/'):
                    event_url = event_url[:-1]
                    
                    
                
                # Loop through pages 1 to 100 in parallel for each event URL (reduced from 1000)
                for page_num in range(1, 2001):
                    page_url = f"{event_url}?page={page_num}"
                    
                    yield scrapy.Request(
                        url=page_url,
                        headers={'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36 Edg/136.0.0.0'},
                        callback=self.parse_event_details,
                        meta={
                            'event_name': event_name,
                            'year': year,
                            'event_title': link_text,
                            'page': page_num
                        },
                        dont_filter=True,
                    )
                
        except Exception as e:
            self.logger.error(f"Error in parse_event_page: {str(e)}")

    def parse_event_details(self, response):
        try:
            event_name = response.meta['event_name']
            year = response.meta['year']
            event_title = response.meta['event_title']
            current_page = response.meta.get('page', 1)
            
            self.logger.info(f"Parsing event details for {event_name} {year}, event title: {event_title}, page: {current_page}")
            
            # Check if there are table rows with individual result links
            table_rows = response.xpath('//tr//a[contains(@href, "/results/individuals/")]')
            
            if not table_rows:
                self.logger.info(f"No table rows found on page {current_page}, skipping")
                return
            
            # Extract individual result links directly from table rows
            individual_links = response.xpath('//tr//a[contains(@href, "/results/individuals/")]/@href').getall()
            
            for link in individual_links:
                # Extract individual ID from href like /races/sydney-marathon-2024/events/1/results/individuals/8
                match = re.search(r'/results/individuals/(\d+)', link)
                if match:
                    individual_id = match.group(1)
                    
                    if individual_id not in self.processed_individuals:
                        self.processed_individuals.add(individual_id)
                        
                        individual_url = urljoin(response.url, link)
                        
                        yield scrapy.Request(
                            url=individual_url,
                            headers={'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36 Edg/136.0.0.0'},
                            callback=self.parse_individual_result,
                            meta={
                                'event_name': event_name,
                                'year': year,
                                'event_title': event_title,
                                'individual_id': individual_id
                            },
                            dont_filter=True
                        )
                
        except Exception as e:
            self.logger.error(f"Error in parse_event_details: {str(e)}")

    def parse_individual_result(self, response):
        try:
            event_name = response.meta['event_name']
            year = response.meta['year']
            event_title = response.meta['event_title']
            individual_id = response.meta['individual_id']
            
            self.logger.info(f"Parsing individual result for {individual_id}")
            
            # Extract event_id from URL path (number after /events/)
            original_event_id = None
            url_match = re.search(r'/events/(\d+)', response.url)
            if url_match:
                original_event_id = url_match.group(1)
            
            # Create unique event_id by combining event_name, year, and original_event_id
            event_id = f"{event_name}_{year}_{original_event_id}" if original_event_id else f"{event_name}_{year}"
            
            # Create unique master_event_id (same as event_id for this spider)
            master_event_id = event_id
            
            # Extract runner name from the div with class g-font-size-30
            runner_name = response.xpath('//div[@class="g-font-size-30"]/text()').get()
            
            # Clean up runner name - remove extra whitespace, newlines, and bib number in parentheses
            if runner_name:
                runner_name = ' '.join(runner_name.split())  # Remove extra whitespace and newlines
                # Remove bib number in parentheses like "(#131)"
                runner_name = re.sub(r'\s*\(#[^)]+\)\s*', '', runner_name)
                runner_name = runner_name.strip()
            
            # Extract bib number - look for patterns like (#150) or similar
            bib_number = response.xpath('//span[contains(text(), "#")]/text()').get()
            if not bib_number:
                bib_number = response.xpath('//text()[contains(., "#")]').get()
            
            if bib_number:
                # Extract just the number from patterns like "#150" or "(#150)"
                bib_match = re.search(r'#?(\d+)', bib_number)
                if bib_match:
                    bib_number = bib_match.group(1)
            
            # Extract times using XPath - handle both cases
            net_time = ""
            gun_time = ""
            
            # Look for NET TIME
            net_time_div = response.xpath('//div[contains(@class, "g-font-size-25") and contains(text(), "NET TIME")]')
            if net_time_div:
                net_time_text = ''.join(net_time_div.xpath('.//text()').getall())
                # Extract time using regex - look for HH:MM:SS pattern
                time_match = re.search(r'(\d{2}:\d{2}:\d{2})', net_time_text)
                if time_match:
                    net_time = time_match.group(1)
            
            # Look for GUN TIME
            gun_time_div = response.xpath('//div[contains(@class, "g-font-size-25") and contains(text(), "GUN TIME")]')
            if gun_time_div:
                gun_time_text = ''.join(gun_time_div.xpath('.//text()').getall())
                # Extract time using regex - look for HH:MM:SS pattern
                time_match = re.search(r'(\d{2}:\d{2}:\d{2})', gun_time_text)
                if time_match:
                    gun_time = time_match.group(1)
            
            # Look for single TIME (for finished runners)
            time_div = response.xpath('//div[contains(@class, "g-font-size-25") and contains(text(), "TIME") and not(contains(text(), "NET TIME")) and not(contains(text(), "GUN TIME"))]')
            if time_div:
                time_text = ''.join(time_div.xpath('.//text()').getall())
                # Extract time using regex - look for HH:MM:SS pattern
                time_match = re.search(r'(\d{2}:\d{2}:\d{2})', time_text)
                if time_match:
                    # If we found a single TIME but no NET TIME, use it as net_time
                    if not net_time:
                        net_time = time_match.group(1)
                    # If we found a single TIME but no GUN TIME, use it as gun_time too
                    if not gun_time:
                        gun_time = time_match.group(1)
            
            # Use net_time as finish_time for backward compatibility
            finish_time = net_time
            
            # Debug logging
            self.logger.info(f"Extracted times - Net: {net_time}, Gun: {gun_time}")
            self.logger.info(f"Found NET TIME divs: {len(net_time_div)}, GUN TIME divs: {len(gun_time_div)}, TIME divs: {len(time_div)}")
            
            # Extract rankings from the div structure
            rank_overall = ""
            rank_gender = ""
            rank_category = ""
            age_category = ""
            gender = ""
            
            # Extract rankings using XPath - simplified approach
            # Look for all ranking divs
            ranking_divs = response.xpath('//div[contains(@class, "d-flex justify-content-between")]//div[contains(@class, "g-line-height-1")]')
            
            self.logger.info(f"Found {len(ranking_divs)} ranking divs")
            
            for div in ranking_divs:
                title = div.xpath('.//h5/text()').get()
                if not title:
                    continue
                
                title = title.strip()
                self.logger.info(f"Processing ranking div with title: {title}")
                
                # Get the ranking numbers
                rank_num = div.xpath('.//b/text()').get()
                total_num = div.xpath('.//span/text()').get()
                
                self.logger.info(f"Rank num: {rank_num}, Total num: {total_num}")
                
                if rank_num and total_num:
                    # Clean up the total number (remove "OF" and extra spaces)
                    if 'OF' in total_num:
                        total_num = total_num.split('OF')[-1].strip()
                    
                    ranking = f"{rank_num}/{total_num}"
                    
                    if title == "Place":
                        rank_overall = ranking
                    elif title in ["Male", "Female"]:
                        rank_gender = ranking
                        gender = title  # Extract gender from the ranking div
                    else:
                        # This is likely an age category
                        age_category = title
                        rank_category = ranking
                else:
                    # For DNS/DNF cases, extract gender and age category from titles even if rankings are empty
                    if title in ["Male", "Female"]:
                        gender = title
                    elif title and "-" in title:  # Age category like "35-39"
                        age_category = title
            
            # Extract nationality/country using XPath
            nationality = ""
            flag_element = response.xpath('//span[contains(@class, "flag-")]/@class').get()
            if flag_element:
                # Extract country code from class like "flag flag-2x flag-aus"
                if 'flag-' in flag_element:
                    flag_parts = flag_element.split('flag-')
                    if len(flag_parts) > 1:
                        nationality = flag_parts[-1].split()[0].upper()
            
          
            
            race_name = response.xpath('//h2[@id="raceHeaderText"]/text()').get()
            if race_name:
                race_name = race_name.strip()
            else:
                race_name = ''  
            
            race_date = response.xpath('//div[@class="col-auto mr-md-auto g-px-15"]/text()').get()
            if race_date:
                race_date = race_date.strip()
                date_match = re.search(r'([^|]+)', race_date)
                if date_match:
                    date_text = date_match.group(1).strip()
                    try:
                        date_clean = re.sub(r'(\d+)(st|nd|rd|th)', r'\1', date_text)
                        parsed_date = datetime.strptime(date_clean, "%d %B %Y")
                        race_date = parsed_date.strftime("%Y-%m-%d")
                    except ValueError:
                        race_date = date_text
            else:
                race_date = ""
            
            result = {
                'summary': {
                    'event_id': event_id,
                    'race_name': race_name,
                    'race_date': race_date, 
                    'master_event_id': master_event_id
                },
                'bib_number': bib_number or '',
                'distance_category': event_title,
                'runner_name': runner_name or '',
                'gender': gender,
                'age_category': age_category or '',
                'finish_time_net': net_time or '',
                'finish_time_gun': gun_time or '',  
                'chip_pace': '', 
                'rank_overall': rank_overall or '',
                'rank_gender': rank_gender or '',
                'rank_age_category': rank_category or '',
                'jsonb': {
                    'bib': bib_number or '',
                    'nationality': nationality or '',
                    'age': '',  
                    'race_category': event_title,
                    'original_name': runner_name or '',
                    'overall_rank': rank_overall.split('/')[0] if rank_overall else '',
                    'gender': gender,
                    'net_time': net_time or '',
                    'gross_time': gun_time or ''
                }
            }
            
            # Debug logging for final result
            self.logger.info(f"Final result - Runner: {runner_name}, Gender: {gender}, Age: {age_category}")
            self.logger.info(f"Final result - Net time: {net_time}, Gun time: {gun_time}")
            self.logger.info(f"Final result - Rankings: Overall={rank_overall}, Gender={rank_gender}, Category={rank_category}")
            
            yield result
            
        except Exception as e:
            self.logger.error(f"Error in parse_individual_result for {response.meta['individual_id']}: {str(e)}") 