import scrapy
import re
from datetime import datetime

class BerlinMikatimingSpider(scrapy.Spider):
    name = "berlin_mikatiming"
    custom_settings = {
        'AUTOTHROTTLE_ENABLED': False,
        'CONCURRENT_REQUESTS': 16,
        # 'CONCURRENT_REQUESTS_PER_DOMAIN': 100,
        # 'CONCURRENT_REQUESTS_PER_IP': 100,
        'RETRY_ENABLED': True,
        #'LOG_FILE': f"logs/berlin_mikatiming.log",
        'USER_AGENT': "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36 Edg/136.0.0.0"
    }

    def start_requests(self):
        self.headers = {
            
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-encoding": "gzip, deflate, br, zstd",
            "accept-language": "en-US,en;q=0.9",
            "priority": "u=0, i",
            "sec-ch-ua": "\"Chromium\";v=\"140\", \"Not=A?Brand\";v=\"24\", \"Microsoft Edge\";v=\"140\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"macOS\"",
            "sec-fetch-dest": "document",
            "sec-fetch-mode": "navigate",
            "sec-fetch-site": "none",
            "sec-fetch-user": "?1",
            "upgrade-insecure-requests": "1",
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36 Edg/140.0.0.0"
        }
        

   
        for i in range(1, 501):
            for gender in ['M', 'W']:
                url = f'https://berlin.r.mikatiming.com/2025/?page={i}&pid=list&pidp=start'
                self.payload = {
                    "lang": "EN_CAP",
                    "startpage": "start_responsive",
                    "startpage_type": "lists",
                    "event_main_group": "BMW BERLIN MARATHON",
                    "event": "BML_HCH3C0OH2F2",
                    "search[sex]": gender,
                    "search[age_class]": "%",
                    "num_results": "100",
                    "submit": "submit"
                }
                yield scrapy.FormRequest(
                    url=url,
                    method='POST',
                    headers=self.headers,
                    formdata=self.payload,
                    callback=self.get_table_list
                )
    
    def get_table_list(self, response):
        for li in response.xpath("//ul/li[contains(@class, 'list-group-item')]"):
            place = li.xpath(".//div[contains(@class, 'place-primary')]/text()").get()
            fullname = li.xpath(".//h4[contains(@class, 'type-fullname')]/a/text()").get()
            detail_link = li.xpath(".//h4[contains(@class, 'type-fullname')]/a/@href").get()
            bib_number = li.xpath(".//div[contains(@class, 'type-field')][1]/text()").get()
            age_group = li.xpath(".//div[contains(@class, 'type-age_class')]/text()").get()
            club = li.xpath(".//div[contains(@class, 'type-field')]/span/text()").get()
            gun_time = li.xpath(".//div[contains(@class, 'type-time')][div='Gun time']/text()").get()
            finish_time = li.xpath(".//div[contains(@class, 'type-time')][div='Finish']/text()").get()

            # follow detail link
            if detail_link:
                detail_url = response.urljoin(detail_link)
                yield scrapy.Request(
                    url=detail_url,
                    headers=self.headers,
                    callback=self.get_bib_details,
                    meta={
                        "event_id": "2025_Berlin",  
                        "place": place,
                        "fullname": fullname,
                        "bib_number": bib_number.strip() if bib_number else None,
                        "age_group": age_group,
                        "club": club,
                        "gun_time": gun_time,
                        "finish_time": finish_time
                    }
                )
    

    def get_bib_details(self, response):
        race_info = {
            "event_id": response.meta["event_id"],
            "race_name": "BMW BERLIN-MARATHON",
            "race_date": "2025-09-28T00:00:00",  # static for now; can scrape header if needed
        }
        master_event_id = response.meta["event_id"]

        # --- General info ---
        fullname = response.xpath("//tr[@class=' f-__fullname']/td/text()").get()
        bib_number = response.xpath("//tr[@class=' f-start_no_text']/td/text()").get()
        distance_category = response.xpath("//tr[@class='list-highlight f-event_name']/td/text()").get()
        age_category = response.xpath("//tr[@class=' f-age_class_desc']/td/text()").get()

        # --- Results ---
        rank_overall = response.xpath("//tr[@class=' f-place_nosex']/td/text()").get()
        rank_gender = response.xpath("//tr[@class=' f-place_all']/td/text()").get()
        rank_age_category = response.xpath("//tr[@class='list-highlight f-place_age']/td/text()").get()
        finish_time_net = response.xpath("//tr[@class='list-highlight f-time_finish_netto']/td/text()").get()
        finish_time_gun = response.xpath("//tr[@class=' f-time_finish_brutto']/td/text()").get()

        # --- Parse nationality from fullname e.g. "Simpson, Vicky (GBR)" ---
        nationality = ""
        runner_name = fullname
        m = re.match(r"^(.*)\s\((\w{3})\)$", fullname or "")
        if m:
            runner_name = m.group(1).strip()
            nationality = m.group(2)

        # --- Gender (heuristic: from rank_gender string or leave blank) ---
        gender = ""
        if rank_gender:
            # Sometimes shown like "100/500 (F)" etc.
            g = re.search(r"\((M|F)\)", rank_gender)
            if g:
                gender = "Male" if g.group(1) == "M" else "Female"

            # --- Final dict in your desired schema ---
            result = {
                "summary": {
                    "event_id": self.payload['event'],
                    "race_name": race_info["race_name"],
                    "race_date": '2025-01-01',
                    "master_event_id": self.payload['event'],
                },
                "bib_number": str(bib_number).strip() if bib_number else response.meta.get("bib_number"),
                "distance_category": distance_category,
                "runner_name": runner_name,
                "gender": self.payload.get("search[sex]"),
                "age_category": str(age_category or ""),
                "finish_time_net": finish_time_net,
                "finish_time_gun": finish_time_gun,
                "chip_pace": "",
                "rank_overall": rank_overall or "",
                "rank_gender": rank_gender or "",
                "rank_age_category": rank_age_category or "",
                "jsonb": {
                    "bib": str(bib_number).strip() if bib_number else response.meta.get("bib_number"),
                    "nationality": nationality,
                    "age": str(age_category or ""),
                    "race_category": distance_category,
                    "original_name": fullname,
                    "overall_rank": str(rank_overall or ""),
                    "gender": gender,
                    "net_time": finish_time_net,
                    "gross_time": finish_time_gun,
                },
            }

            yield result


