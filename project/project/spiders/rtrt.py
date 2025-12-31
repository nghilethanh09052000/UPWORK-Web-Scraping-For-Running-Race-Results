import scrapy
import json
import re


class RtrtSpider(scrapy.Spider):
    name = "rtrt"
    
    custom_settings = {
        'AUTOTHROTTLE_ENABLED': False,
        'CONCURRENT_REQUESTS': 100,
        'RETRY_ENABLED': True,
        'LOG_FILE': f"logs/rtrt.log",
        'USER_AGENT': "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36 Edg/143.0.0.0",
        'COOKIES_ENABLED': True,
    }

    def __init__(self, *args, **kwargs):
        super(RtrtSpider, self).__init__(*args, **kwargs)
        self.base_url = "https://api.rtrt.me"
        # Get parameters from command line or use defaults
        self.event_slug = getattr(self, 'event_slug', 'ATC-PEACHTREE-2025')
        self.category_slug = getattr(self, 'category_slug', 'overall-by-net-time-all-participants-10k%3A_ALL')
        self.split_point = getattr(self, 'split_point', 'FINISH')
        self.appid = getattr(self, 'appid', '65960b635df5fd7789550811')
        self.token = getattr(self, 'token', '0EB44AF0C189CAD93F5F')
        self.max_results = int(getattr(self, 'max_results', '15'))
        self.race_name = getattr(self, 'race_name', 'Peachtree Road Race')

    def start_requests(self):
        """Start with initial POST request"""
        api_url = f"{self.base_url}/events/{self.event_slug}/categories/{self.category_slug}/splits/{self.split_point}"
        
        # Initial request without start parameter
        formdata = {
            'timesort': '1',
            'uselbhide': '1',
            'checksum': '',
            'appid': self.appid,
            'token': self.token,
            'max': str(self.max_results),
            'catloc': '1',
            'cattotal': '1',
            'units': 'standard',
            'source': 'webtracker',
        }
        
        yield scrapy.FormRequest(
            url=api_url,
            formdata=formdata,
            callback=self.parse_api_response,
            meta={
                'start': 1,
                'checksum': '',
            }
        )

    def parse_api_response(self, response):
        """Parse JSON API response"""
        try:
            data = json.loads(response.text)
        except json.JSONDecodeError:
            self.logger.error(f"Failed to parse JSON from {response.url}")
            return
        
        # Extract checksum from response for next requests
        checksum = data.get('info', {}).get('checksum', '')
        
        # Extract total participants from info
        cattotal = int(data.get('info', {}).get('cattotal', 0))
        current_start = response.meta.get('start', 1)
        
        self.logger.info(f"Parsing API response: start={current_start}, total={cattotal}, found={len(data.get('list', []))}")
        
        # Process each runner in the list
        for runner in data.get('list', []):
            # Extract gender
            gender = ''
            sex = runner.get('sex', '')
            if sex == 'M':
                gender = 'Male'
            elif sex == 'F':
                gender = 'Female'
            elif sex == 'NB':
                gender = 'Non-Binary'
            
            # Extract distance category from course
            distance_category = runner.get('course', '')
            if distance_category == '10k':
                distance_category = '10km'
            elif distance_category == '5k':
                distance_category = '5km'
            elif 'marathon' in distance_category.lower():
                distance_category = 'Marathon'
            elif 'half' in distance_category.lower():
                distance_category = 'Half Marathon'
            
            # Extract net time and gun time
            net_time = runner.get('netTime', '')
            gun_time = runner.get('waveTime', '')  # waveTime seems to be gun time
            
            # Extract rankings
            rank_overall = runner.get('place', '')
            rank_gender = ''  # Not directly available, would need separate API call
            rank_age_category = ''  # Not directly available, would need separate API call
            
            # Extract nationality
            nationality = runner.get('country_iso', '').upper() if runner.get('country_iso') else runner.get('country', '')
            
            # Extract age from bib_display (e.g., "Bib 2106  M-21  Atlanta, GA USA" -> "21")
            age = ''
            bib_display = runner.get('bib_display', '')
            if bib_display:
                # Try to extract age from bib_display (e.g., "M-21" or "F-30")
                age_match = re.search(r'[MFNB]-(\d+)', bib_display)
                if age_match:
                    age = age_match.group(1)
            
            # Build result in standard format matching sportstimingsolutions.py
            result = {
                'summary': {
                    'event_id': runner.get('pid', ''),
                    'race_name': self.race_name,
                    'race_date': '',  # Not available in API response
                    'master_event_id': f"rtrt_{self.event_slug}",
                },
                'bib_number': runner.get('bib', ''),
                'distance_category': distance_category,
                'runner_name': runner.get('name', ''),
                'gender': gender,
                'age_category': '',  
                'finish_time_net': net_time,
                'finish_time_gun': gun_time,
                'chip_pace': runner.get('kmPace', '') or '',  
                'rank_overall': rank_overall or '',
                'rank_gender': rank_gender or '',
                'rank_age_category': rank_age_category or '',
                'jsonb': {
                    'bib': runner.get('bib', ''),
                    'nationality': nationality,
                    'age': age,  
                    'race_category': runner.get('division', ''),
                    'original_name': runner.get('name', ''),
                    'overall_rank': rank_overall.split('/')[0] if rank_overall and '/' in rank_overall else (rank_overall or ''),
                    'gender': gender,
                    'net_time': net_time,
                    'gross_time': gun_time
                }
            }
            
            yield result
        
        # Check if we need to fetch more results
        next_start = current_start + self.max_results
        if next_start <= cattotal:
            # Make next request with incremented start
            formdata = {
                'timesort': '1',
                'uselbhide': '1',
                'checksum': checksum,
                'appid': self.appid,
                'token': self.token,
                'max': str(self.max_results),
                'start': str(next_start),
                'catloc': '1',
                'cattotal': '1',
                'units': 'standard',
                'source': 'webtracker',
            }
            
            yield scrapy.FormRequest(
                url=response.url,
                formdata=formdata,
                callback=self.parse_api_response,
                meta={
                    'start': next_start,
                    'checksum': checksum,
                }
            )

