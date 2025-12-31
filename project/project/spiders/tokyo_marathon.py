import scrapy
import json
import logging
import re
from urllib.parse import urljoin


class TokyoMarathonSpider(scrapy.Spider):
    name = "tokyo_marathon"

    # Custom settings for this spider
    custom_settings = {
        'AUTOTHROTTLE_ENABLED': False,
        'CONCURRENT_REQUESTS': 16,
        # 'CONCURRENT_REQUESTS_PER_DOMAIN': 100,
        # 'CONCURRENT_REQUESTS_PER_IP': 100,
        'RETRY_ENABLED': True,
        #'LOG_FILE': f"logs/berlin_mikatiming.log",
        'USER_AGENT': "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36 Edg/136.0.0.0",
        'DOWNLOAD_DELAY': 1 
    }


    def clean_rank(self, text):
        if not text:
            return ""
        # replace any whitespace around '／' with '/'
        return re.sub(r"\s*／\s*", "/", text.strip())
 

    def start_requests(self):
        """Start with the main result page"""
        self.start_urls = "https://www.marathon.tokyo/2025/result/index.php"
        for page in range(1, 1001):
            formdata = {
                'category': '',
                'number': '',
                'name': '',
                'age': '',
                'country': '',
                'prefecture': '',
                'sort_key': '',
                'place': '',
                'sort_asc': '',
                'page': str(page),
                'd_number': ''
            }
            yield scrapy.FormRequest(
                url=self.start_urls,
                callback=self.parse_main_page,
                method='POST',
                headers={
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                    'Accept-Language': 'en-US,en;q=0.9',
                    'Cache-Control': 'max-age=0',
                    'Sec-Fetch-Dest': 'document',
                    'Sec-Fetch-Mode': 'navigate',
                    'Sec-Fetch-Site': 'none',
                    'Sec-Fetch-User': '?1',
                    'Upgrade-Insecure-Requests': '1',
                },
                formdata=formdata,
                meta={'page': str(page) }
            )

    def parse_main_page(self, response):
        """Parse the main result page and extract form data"""
        self.logger.info(f"Parsing main page: {response.url}")
    
        # each table row
        rows = response.xpath("//tr")
        self.logger.info(f"Found {len(rows)} rows")

        for row in rows:
            tds = row.xpath("./td")
            if len(tds) < 8:
                continue 

            runner = {
                "overall_place": tds[0].xpath("normalize-space(.)").get(),
                "category": tds[1].xpath("normalize-space(.)").get(),
                "bib_number": tds[2].xpath("normalize-space(.)").get(),
                "name": " ".join(tds[3].xpath(".//text()").getall()).strip(),
                "age": tds[4].xpath("normalize-space(.)").get(),
                "sex": tds[5].xpath("normalize-space(.)").get(),
                "country": tds[6].xpath("normalize-space(.)").get(),
            }

            # extract detail id from javascript:detail('5');
            href = tds[3].xpath("./a/@href").get()
            if href and "detail(" in href:
                detail_id = href.split("detail('")[-1].split("')")[0]

 
            formdata = {
                'category': '',
                'number': '',
                'name': '',
                'age': '',
                'country': '',
                'prefecture': '',
                'sort_key': '',
                'place': '',
                'sort_asc': '',
                'page': response.meta['page'],
                "d_number": detail_id, 
            }

            yield scrapy.FormRequest(
                url="https://www.marathon.tokyo/2025/result/detail.php",
                formdata=formdata,
                callback=self.parse_results_details,
                headers={
                    'Content-Type': 'application/x-www-form-urlencoded',
                    'Origin': 'https://www.marathon.tokyo',
                    'Referer': response.url,
                },
                meta={"runner": runner}
            )

    def safe_extract(self, sel, default=""):
        """Helper to safely extract text"""
        text = sel.get()
        return text.strip() if text else default

    def parse_results_details(self, response):
        """Parse the results index page to extract runner data and pagination"""
        """Parse the runner detail page"""
        runner = response.meta["runner"]
        bib_number = runner.get("bib_number")
        name = runner.get("name")
        distance_category = runner.get("category")
        gender = runner.get("sex")
        nationality = runner.get("country")
        age = runner.get('age')
        

        category_text = response.xpath(
            "//td[contains(., 'Category Place')]/text()"
        ).get()

        rank_overall = ""
        if category_text:
            # match numbers after colon (or full-width colon)
            m = re.search(r"[:：]\s*([0-9]+\s*／\s*[0-9]+)", category_text)
            if m:
                rank_overall = self.clean_rank(m.group(1).strip())

        age_text = response.xpath(
            "//td[contains(., 'Age Place')]/text()"
        ).get()
        
        m1 = re.search(r"\((.*?)\)", age_text)
        if m1:
            age_category = self.clean_rank(m1.group(1))

        m2 = re.search(r"[:：]\s*([^)]+)$", age_text)
        if m2:
            rank_age_category = m2.group(1).strip()
            
        gender_text = response.xpath(
            "//td[contains(., 'Gender Place')]/text()"
        ).get()

        rank_gender = ""

        if gender_text:
            m = re.search(r"[:：]\s*([0-9／]+)", gender_text)
            if m:
                rank_gender = self.clean_rank(m.group(1).strip())

        finish_time_net = response.xpath(
            "//th[contains(., 'Time (net)')]/following-sibling::td[1]/text()"
        ).re_first(r"\d{2}:\d{2}:\d{2}")

        finish_time_gun = response.xpath(
            "//th[contains(., 'Time (gross)')]/following-sibling::td[1]/text()"
        ).re_first(r"\d{2}:\d{2}:\d{2}")
        
        result = {
            "summary": {
                "event_id": self.payload['event'] if hasattr(self, "payload") else "tokyo2025",
                "race_name": 'Tokyo Marathon 2025',
                "race_date": "2025-01-01", 
                "master_event_id": self.payload['event'] if hasattr(self, "payload") else "tokyo2025",
            },
            "bib_number": bib_number,
            "distance_category": distance_category,
            "runner_name": name,
            "gender": gender,
            "age_category": age_category,
            "finish_time_net": finish_time_net,
            "finish_time_gun": finish_time_gun ,
            "chip_pace": "",
            "rank_overall": rank_overall,
            "rank_gender": rank_gender,
            "rank_age_category": rank_age_category,
            "jsonb": {
                "bib": bib_number,
                "nationality": nationality,
                "age": age,
                "race_category": distance_category,
                "original_name": name,
                "overall_rank": rank_overall,
                "gender": gender,
                "net_time": finish_time_net,
                "gross_time": finish_time_gun,
            }
        }

        yield result
