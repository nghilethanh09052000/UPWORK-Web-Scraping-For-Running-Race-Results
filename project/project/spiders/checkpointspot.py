import scrapy
import re


class CheckpointspotSpider(scrapy.Spider):
    name = "checkpointspot"
    
    custom_settings = {
        'AUTOTHROTTLE_ENABLED': False,
        'CONCURRENT_REQUESTS': 16,
        'RETRY_ENABLED': True,
        'LOG_FILE': f"logs/checkpointspot.log",
        'USER_AGENT': "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36 Edg/143.0.0.0",
        #'DOWNLOAD_DELAY': 1,
        'COOKIES_ENABLED': True,
    }

    def __init__(self, *args, **kwargs):
        super(CheckpointspotSpider, self).__init__(*args, **kwargs)
        self.base_url = "https://results.checkpointspot.asia"
        # Get parameters from command line or use defaults
        self.cid = getattr(self, 'cid', '17036')
        self.rid = getattr(self, 'rid', '10516')
        # EId mapping: 1=Marathon(42km), 2=Half Marathon(21km), 3=10km
        self.eid_list = getattr(self, 'eid_list', None)
        if self.eid_list:
            # If eid_list is provided as comma-separated string, convert to list
            if isinstance(self.eid_list, str):
                self.eid_list = [eid.strip() for eid in self.eid_list.split(',')]
        else:
            # Default: loop through all event IDs
            self.eid_list = ['1', '2', '3']
        self.max_pages = int(getattr(self, 'max_pages', '50'))
        
        # Distance category mapping
        self.distance_map = {
            '1': 'Marathon',  # 42km
            '2': 'Half Marathon',  # 21km
            '3': '10km'  # 10km
        }

    def get_headers(self, referer=None):
        """Get standard headers for requests"""
        headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            #'Accept-Encoding': 'gzip, deflate, br, zstd',
            'Accept-Language': 'en-US,en;q=0.9',
            'Cookie': 'ASP.NET_SessionId=zasovyianaw1q13voq5jn2ti; _gid=GA1.2.1112834768.1767163070; _gat_gtag_UA_40561276_1=1; _ga=GA1.1.926200266.1767163070; cf_clearance=YaH6Ce_VJrK2ZadgTFr9BnXvi010Kj5axmpG35uqOok-1767164603-1.2.1.1-j5GIFyw4trmIM7M8JsqJt9zzszr8vHX7zwtMdjd0ejsNdnKrZAtVb9St6PlX4G2JPsb_6yhUSSm1DnDBzW8xd9HbBNNF5zzXLgfFK7TESaO8MTMDczYSLgWbjK3Vc2bI1xV5tX1_UTbIUDbnPYWwvuimPnATqVers0batqdu6FjLBuq2l4IuFKNX8SAOgz_DcCBO1twXBxmyENWv941ZMpvcK2oiTWESQh3cvN_BwDg; _ga_6F4SKJ6H66=GS2.1.s1767163069$o1$g1$t1767164623$j40$l0$h0',
            'Priority': 'u=0, i',
            'Sec-CH-UA': '"Microsoft Edge";v="143", "Chromium";v="143", "Not A(Brand";v="24"',
            'Sec-CH-UA-Mobile': '?0',
            'Sec-CH-UA-Platform': '"macOS"',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none' if not referer else 'same-origin',
            'Sec-Fetch-User': '?1',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36 Edg/143.0.0.0',
        }
        if referer:
            headers['Referer'] = referer
        return headers

    def start_requests(self):
        """Start with paginated results pages for each event ID"""
        # Loop through each event ID (EId)
        for eid in self.eid_list:
            distance_category = self.distance_map.get(eid, 'Unknown')
            self.logger.info(f"Starting requests for EId={eid} ({distance_category})")
            
            # Loop through pages for this event
            for page_num in range(1, self.max_pages + 1):
                results_url = f"{self.base_url}/results.aspx?CId={self.cid}&RId={self.rid}&EId={eid}&dt=0&PageNo={page_num}"
                
                yield scrapy.Request(
                    url=results_url,
                    callback=self.parse_results_page,
                    headers=self.get_headers(referer=f'{self.base_url}/results.aspx'),
                    meta={
                        'page': page_num,
                        'cid': self.cid,
                        'rid': self.rid,
                        'eid': eid,
                        'distance_category': distance_category,
                    }
                )

    def parse_results_page(self, response):
        """Parse the results page and extract UIDs using regex"""
        self.logger.info(f"Parsing results page {response.meta['page']}: {response.url}")

        # Extract all UIDs from the HTML using regex
        # Pattern matches: uid=17036-10516-1-1692335 or uid="17036-10516-1-1692335"
        uid_pattern = r'uid=([0-9]+-[0-9]+-[0-9]+-[0-9]+)'
        uids = re.findall(uid_pattern, response.text)
        
        # Remove duplicates while preserving order
        unique_uids = list(dict.fromkeys(uids))
        
        self.logger.info(f"Found {len(unique_uids)} unique UIDs on page {response.meta['page']}")
        
        # For each UID, construct detail URL and request detail page
        for uid in unique_uids:
            detail_url = f"{self.base_url}/myresults.aspx?uid={uid}"
            
            yield scrapy.Request(
                url=detail_url,
                callback=self.parse_runner_detail,
                headers=self.get_headers(referer=response.url),
                meta={
                    'cid': response.meta['cid'],
                    'rid': response.meta['rid'],
                    'eid': response.meta['eid'],
                    'distance_category': response.meta['distance_category'],
                }
            )

    def parse_runner_detail(self, response):
        """Parse detailed runner information from detail page"""
        self.logger.info(f"Parsing runner detail: {response.url}")
        
        try:
            # Extract name
            runner_name = response.css('#ctl00_Content_Main_lblName::text').get()
            runner_name = runner_name.strip() if runner_name else ''
            
            # Extract bib number
            bib_number = response.css('#ctl00_Content_Main_lblRaceNo::text').get()
            bib_number = bib_number.strip() if bib_number else ''
            
            # Extract event and date
            event = response.css('#ctl00_Content_Main_lblEvent::text').get()
            event = event.strip() if event else ''
            
            race_date = response.css('#ctl00_Content_Main_lblEventDate::text').get()
            race_date = race_date.strip() if race_date else ''
            
            # Extract data from bio table
            bio_rows = response.css('#ctl00_Content_Main_grdBio tr')
            gender = ''
            category = ''
            status = ''
            country = ''
            
            for row in bio_rows:
                label = row.css('td:first-child::text').get()
                value = row.css('td:last-child::text').get()
                
                if label and value:
                    label = label.strip()
                    value = value.strip()
                    
                    if 'Gender' in label:
                        gender = value
                    elif 'Category' in label:
                        category = value
                    elif 'Status' in label:
                        status = value
                    elif 'Country' in label:
                        country = value
            
            # Extract nationality from flag image if not found in table
            nationality = country
            if not nationality:
                flag_img = response.css('#ctl00_Content_Main_imgFlag::attr(src)').get()
                if flag_img:
                    flag_match = re.search(r'flags/[^/]+/([A-Z]{2})\.png', flag_img)
                    if flag_match:
                        nationality = flag_match.group(1)
            
            # Extract finish time (Gun Time)
            finish_time_gun = response.css('#ctl00_Content_Main_lblTime1Large::text').get()
            if not finish_time_gun:
                finish_time_gun = response.css('#ctl00_Content_Main_lblTime1Small::text').get()
            finish_time_gun = finish_time_gun.strip() if finish_time_gun else ''
            
            # Extract net time
            finish_time_net = response.css('#ctl00_Content_Main_lblTime2Large::text').get()
            if not finish_time_net:
                finish_time_net = response.css('#ctl00_Content_Main_lblTime2Small::text').get()
            finish_time_net = finish_time_net.strip() if finish_time_net else ''
            
            # Extract ranks (Gun Time)
            rank_overall_gun = response.css('#ctl00_Content_Main_lblOPos1::text').get()
            rank_overall_gun = rank_overall_gun.strip() if rank_overall_gun else ''
            
            rank_category_gun = response.css('#ctl00_Content_Main_lblCPos1::text').get()
            rank_category_gun = rank_category_gun.strip() if rank_category_gun else ''
            
            rank_gender_gun = response.css('#ctl00_Content_Main_lblGPos1::text').get()
            rank_gender_gun = rank_gender_gun.strip() if rank_gender_gun else ''
            
            # Extract ranks (Net Time)
            rank_overall_net = response.css('#ctl00_Content_Main_lblOPos2::text').get()
            rank_overall_net = rank_overall_net.strip() if rank_overall_net else ''
            
            rank_category_net = response.css('#ctl00_Content_Main_lblCPos2::text').get()
            rank_category_net = rank_category_net.strip() if rank_category_net else ''
            
            rank_gender_net = response.css('#ctl00_Content_Main_lblGPos2::text').get()
            rank_gender_net = rank_gender_net.strip() if rank_gender_net else ''
            
            # Extract splits
            splits = []
            split_rows = response.css('#ctl00_Content_Main_divSplitGrid table tbody tr')
            for row in split_rows:
                # Skip header row
                row_style = row.attrib.get('style', '')
                if 'background-color:#0769AD' in row_style or 'background-color: #0769AD' in row_style:
                    continue
                
                split_name = row.css('td:first-child::text').get()
                split_name = split_name.strip() if split_name else ''
                
                # Time of Day (second column, first line)
                time_of_day = row.css('td:nth-child(2)::text').get()
                time_of_day = time_of_day.strip() if time_of_day else ''
                
                # Split Time (third column)
                split_time = row.css('td:nth-child(3)::text').get()
                split_time = split_time.strip() if split_time else ''
                
                # Distance (fourth column, first line)
                distance = row.css('td:nth-child(4)::text').get()
                distance = distance.strip() if distance else ''
                
                if split_name:
                    splits.append({
                        'name': split_name,
                        'time_of_day': time_of_day,
                        'split_time': split_time,
                        'distance': distance
                    })
            
            # Extract age category from category string (e.g., "42km Men Veteran" -> "Veteran")
            age_category = ''
            if category:
                # Try to extract age category from category string
                # Examples: "42km Men Veteran", "42km Women Open", "21km Men Senior"
                category_parts = category.split()
                for part in category_parts:
                    if part.lower() in ['open', 'veteran', 'senior', 'junior', 'master', 'elite']:
                        age_category = part
                        break
                    # Check for age ranges like "18-24", "25-29", etc.
                    if re.match(r'^\d+-\d+$', part):
                        age_category = part
                        break
            
            # Extract gender from category if not found in table
            if not gender:
                if category:
                    if 'Men' in category or 'men' in category:
                        gender = 'Male'
                    elif 'Women' in category or 'women' in category or 'Ladies' in category:
                        gender = 'Female'
            
            # Extract event info from URL parameters
            uid_match = re.search(r'uid=([^&]+)', response.url)
            event_id = uid_match.group(1) if uid_match else f"{response.meta['cid']}-{response.meta['rid']}-{response.meta['eid']}"
            
            # Get distance category from EId mapping
            eid = response.meta.get('eid', '1')
            distance_category = response.meta.get('distance_category', self.distance_map.get(eid, 'Marathon'))
            
            # Build result in standard format matching sportstimingsolutions.py
            result = {
                'summary': {
                    'event_id': event_id,
                    'race_name': 'Borneo International Marathon',
                    'race_date': race_date,
                    'master_event_id': f"checkpointspot_{response.meta['cid']}_{response.meta['rid']}",
                },
                'bib_number': bib_number,
                'distance_category': distance_category,
                'runner_name': runner_name,
                'gender': gender,
                'age_category': '',  
                'finish_time_net': finish_time_net,
                'finish_time_gun': finish_time_gun,
                'chip_pace': '',  
                'rank_overall': rank_overall_net or rank_overall_gun or '',
                'rank_gender': rank_gender_net or rank_gender_gun or '',
                'rank_age_category': rank_category_net or rank_category_gun or '',
                'jsonb': {
                    'bib': bib_number,
                    'nationality': nationality,
                    'age': '',  
                    'race_category': category,
                    'original_name': runner_name,
                    'overall_rank': rank_overall_net.split('/')[0] if rank_overall_net and '/' in rank_overall_net else (rank_overall_net or rank_overall_gun or ''),
                    'gender': gender,
                    'net_time': finish_time_net,
                    'gross_time': finish_time_gun
                }
            }
            
            yield result
            
        except Exception as e:
            self.logger.error(f"Error parsing runner detail {response.url}: {str(e)}", exc_info=True)
            return

