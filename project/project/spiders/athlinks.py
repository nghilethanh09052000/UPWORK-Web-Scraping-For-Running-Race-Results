import scrapy
import json
import logging
from urllib.parse import urljoin
from datetime import datetime

class AthlinksMasterSpider(scrapy.Spider):
    name = 'athlinks_master'
    retries = 0
    custom_settings = {
        
        'DOWNLOAD_DELAY': 0,
        'RETRY_ENABLED': True,
        'LOG_FILE': f"logs/athlinks.log",
        'CONCURRENT_REQUESTS': 100,
        #'AUTOTHROTTLE_ENABLED' : True,
        ##'AUTOTHROTTLE_START_DELAY' : 5,
        #'AUTOTHROTTLE_MAX_DELAY' : 60,
        #'AUTOTHROTTLE_TARGET_CONCURRENCY' : 1.0,
        #'AUTOTHROTTLE_DEBUG' : True,
        
        'DOWNLOADER_MIDDLEWARES': {
            #'project.middlewares.CustomProxyMiddleware': 543,
        },
        "USER_AGENT": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36 Edg/135.0.0.0"
    
    }

    def format_time(self, milliseconds):
        if not milliseconds:
            return ''
        seconds = int(milliseconds)
        hours = seconds // 3600
        minutes = (seconds % 3600) // 60
        seconds = seconds % 60
        return f"{hours:02d}:{minutes:02d}:{seconds:02d}"

    def format_pace(self, milliseconds, distance_meters):
        if not milliseconds or not distance_meters:
            return ''
        # Convert milliseconds to minutes
        total_minutes = milliseconds / (60 * 1000)
        # Convert meters to kilometers
        distance_km = distance_meters / 1000
        # Calculate pace per km
        pace_minutes = total_minutes / distance_km
        minutes = int(pace_minutes)
        seconds = int((pace_minutes - minutes) * 60)
        return f"{minutes}:{seconds:02d}"

    def start_requests(self):
        self.master_id = self.master_id
        self.limit = 50
        if not self.master_id:
            self.logger.error("No master_id provided. Use -a master_id=ID")
            return
            
        # Request master event details
        master_url = f"https://alaska.athlinks.com/MasterEvents/Api/{self.master_id}"
        yield scrapy.Request(
            url=master_url,
            callback=self.parse_master_event,
            meta={'master_id': self.master_id}
        )

    def parse_master_event(self, response):
        data = json.loads(response.text)
        master_id = response.meta['master_id']

        if not data.get('success'):
            self.logger.error(f"Failed f34538to get master event data for {master_id}")
            return

        result = data.get('result', {})
        event_races = result.get('eventRaces', [])

        all_courses = []
        for race in event_races:
            race_id = race.get('raceID')
            race_date_str = race.get('raceDate', '')
            race_date = datetime.fromisoformat(race_date_str)
            
            # if master_id == '20238' and str(race_date.year) != "2025": 
            #     print(f"Year {race_date.year}... continue")
            #     continue
            
            if master_id == '34538' and str(race_id) not in ["167062"]: 
                print(f"Master Id master_id: {master_id} ... Race Id: {race_id} Continue...")
                continue

            if master_id == '34792' and str(race_id) not in ["470894", "829442"]: 
                print(f"Master Id master_id: {master_id} ... Race Id: {race_id} Continue...")
                continue

            # Filter events by year for specific master events
            if master_id == '34990' and race_date.year < 2022:  # BARMER Alsterlauf Hamburg 2022 onwards
                print(f"Master Id {master_id} ... Year {race_date.year} before 2022, skipping...")
                continue
                
            if master_id == '34440':  # Standard Chartered Hong Kong Marathon - All years (no filter)
                pass
                
            if master_id == '4476' and race_date.year < 2010:  # Houston Marathon 2010 onwards
                print(f"Master Id {master_id} ... Year {race_date.year} before 2010, skipping...")
                continue
                
            if master_id == '3281' and race_date.year < 2010:  # Marine Corps Marathon 2010 onwards
                print(f"Master Id {master_id} ... Year {race_date.year} before 2010, skipping...")
                continue
                
            if master_id == '3241' and race_date.year < 2010:  # CIM 2010 onwards
                print(f"Master Id {master_id} ... Year {race_date.year} before 2010, skipping...")
                continue

            # New event filters
            if master_id == '34524' and race_date.year != 2017:  # Munich Half 2020 onwards
                print(f"Master Id {master_id} ... Year {race_date.year} before 2020, skipping...")
                continue
                
            if master_id == '34908' and race_date.year < 2025:  # Generali Genève Marathon 2025 only
                print(f"Master Id {master_id} ... Year {race_date.year} before 2025, skipping...")
                continue
                
            if master_id == '187582' and race_date.year < 2018:  # GENERALI Berlin Half Marathon 2018 onwards
                print(f"Master Id {master_id} ... Year {race_date.year} before 2018, skipping...")
                continue
                
            if master_id == '131958' and race_date.year < 2021:  # HASPA Marathon Hamburg 2021 onwards
                print(f"Master Id {master_id} ... Year {race_date.year} before 2021, skipping...")
                continue
                
            if master_id == '34631' and race_date.year < 2022:  # Hella Hamburg Halbmarathon 2022 onwards
                print(f"Master Id {master_id} ... Year {race_date.year} before 2022, skipping...")
                continue
            
            if master_id == '34455' and race_date.year != 2025:  # Sydney Marathon 2025 only
                print(f"Master Id {master_id} ... Year {race_date.year} before 2025, skipping...")
                continue
            
            if master_id == '34504' and race_date.year not in [2024]: # Athen Marathon
                print(f"Master Id {master_id} ... Year {race_date.year} must be on the list 2016, 2017, 2018, 2020, 2021, 2022, 2023, 2024 , skipping...")
                continue

            if race_date.year < 2005:
                continue

            race_info = {
                'event_id': race_id,
                'race_name': race.get('raceName', ''),
                'race_date': race.get('raceDate', '')
            }
            
            for course in race.get('eventCourses', []):
                result_count = course.get('resultCount', 0)
                if result_count == 0:
                    continue

                all_courses.append({
                    'event_course_id': course.get('eventCourseID'),
                    'course_name': course.get('courseName', ''),
                    'race_info': race_info,
                    'result_count': result_count
                })

        for course_data in all_courses:
            yield from self.fetch_course_results(course_data, offset=0)

    def fetch_course_results(self, course_data, offset):
        """
        Helper method to create a results request starting from a specific offset
        """
        race_info = course_data['race_info']
        master_id = self.master_id

        results_url = (
            f"https://results.athlinks.com/event/{race_info['event_id']}"
            f"?eventCourseId={course_data['event_course_id']}&divisionId=&intervalId="
            f"&from={offset}&limit={self.limit}"
        )
        yield scrapy.Request(
            url=results_url,
            callback=self.parse_race_results,
            meta={
                'master_id': master_id,
                'race_info': race_info,
                'event_course_id': course_data['event_course_id'],
                'course_name': course_data['course_name'],
                'offset': offset,
                'limit': self.limit,
                'total_athletes': course_data['result_count']
            }
        )
    
    def parse_race_results(self, response):
        data = json.loads(response.text)
        if not data:
            return

        master_event_id = response.meta['master_id']
        race_info = response.meta['race_info']
        course_name = response.meta['course_name']
        offset = response.meta['offset']
        limit = response.meta['limit']
        total_athletes = response.meta['total_athletes']


        for course in data:
            interval = course.get('interval', {})
            interval_results = interval.get('intervalResults', [])

            self.logger.info(f'Url Crawling {response.url} with length: {len(interval_results)}')

            for result in interval_results:
                bib = result.get('bib', '')
                entry_id = result.get('entryId', 0)

                individual_url = f"https://results.athlinks.com/individual?eventId={race_info['event_id']}&eventCourseId={response.meta['event_course_id']}&bib={bib}&id={entry_id if not bib else ''}"

                yield scrapy.Request(
                    url=individual_url,
                    callback=self.parse_individual_result,
                    meta={
                        'master_id': master_event_id,
                        'race_info': race_info,
                        'course_name': course_name,
                        'result': result
                    }
                )

        # If more athletes are remaining, request the next batch
        next_offset = offset + limit
        if next_offset < total_athletes:
            course_data = {
                'event_course_id': response.meta['event_course_id'],
                'course_name': course_name,
                'race_info': race_info,
                'result_count': total_athletes
            }
            yield from self.fetch_course_results(course_data, offset=next_offset)
    
    def parse_individual_result(self, response):
        data = json.loads(response.text)
     
        if not data:
            self.logger.warning(f"No data received for URL: {response.url}. Retrying...")
            yield scrapy.Request(
                url=response.url,
                callback=self.parse_individual_result,
                meta=response.meta,
            )

        result = response.meta['result']
        race_info = response.meta['race_info']
        course_name = response.meta['course_name']
        master_event_id = response.meta['master_id']
      

        # Extract rank information from brackets
        overall_rank = None
        gender_rank = None
        age_rank = None
        total_athletes = None
        gender_total = None
        age_total = None

        if data.get('intervals'):
            for interval in data['intervals']:
                brackets = interval.get('brackets', [])
                if len(brackets) >= 3:
                    # Assuming fixed order: 0 = OVERALL, 1 = GENDER, 2 = AGE
                    overall_rank = brackets[0].get('rank')
                    total_athletes = brackets[0].get('totalAthletes')

                    gender_rank = brackets[1].get('rank')
                    gender_total = brackets[1].get('totalAthletes')

                    age_rank = brackets[2].get('rank')
                    age_total = brackets[2].get('totalAthletes')
                    
                # if interval.get('brackets'):
                #     for bracket in interval['brackets']:
                #         if bracket.get('bracketType') == 'OVERALL':
                #             overall_rank = bracket.get('rank')
                #             total_athletes = bracket.get('totalAthletes')
                #         elif bracket.get('bracketType') == 'GENDER':
                #             gender_rank = bracket.get('rank')
                #             gender_total = bracket.get('totalAthletes')
                #         else: 
                #             age_rank = bracket.get('rank')
                #             age_total = bracket.get('totalAthletes')

        yield {
            'summary': {
                'event_id': str(race_info['event_id']),
                'race_name': race_info['race_name'],
                'race_date': datetime.strptime(race_info['race_date'].split('T')[0], '%Y-%m-%d').strftime('%B %d, %Y').upper(),
                'master_event_id': master_event_id
            },
            'bib_number': str(result.get('bib')) if result.get('bib', '') else result.get('entry_id'),
            'distance_category': course_name,
            'runner_name': f"{result.get('firstName', '')} {result.get('lastName', '')}".strip(),
            'gender': 'Male' if result.get('gender') == 'M' else 'Female',
            'age_category': f"{result.get('age', '')}",
            'finish_time_net': self.format_time(result.get('time', {}).get('timeInMillis', 0) / 1000),
            'finish_time_gun': "",
            'chip_pace': "",
            'rank_overall': f"{overall_rank}/{total_athletes}" if overall_rank and total_athletes else '',
            'rank_gender': f"{gender_rank}/{gender_total}" if gender_rank and gender_total else '',
            'rank_age_category': f"{age_rank}/{age_total}" if age_rank and age_total else '',
            'jsonb': {
                'bib': str(result.get('bib', '')),
                'nationality': result.get('country', ''),
                'age': str(result.get('age', '')),
                'race_category': course_name,
                'original_name': result.get('displayName', ''),
                'overall_rank': str(overall_rank) if overall_rank else '',
                'gender': 'Male' if result.get('gender') == 'M' else 'Female',
                'net_time': self.format_time(result.get('time', {}).get('timeInMillis', 0) / 1000),
                'gross_time': self.format_time(result.get('time', {}).get('timeInMillis', 0) / 1000)
            }
        }
