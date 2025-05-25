# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class ProjectItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    pass

class RaceResultItem(scrapy.Item):
    master_event_id = scrapy.Field()
    event_id = scrapy.Field()
    race_name = scrapy.Field()
    race_date = scrapy.Field()
    data = scrapy.Field()

class RunnerItem(scrapy.Item):
    bib_number = scrapy.Field()
    distance_category = scrapy.Field()
    runner_name = scrapy.Field()
    gender = scrapy.Field()
    age_category = scrapy.Field()
    finish_time_net = scrapy.Field()
    finish_time_gun = scrapy.Field()
    chip_pace = scrapy.Field()
    rank_overall = scrapy.Field()
    rank_gender = scrapy.Field()
    rank_age_category = scrapy.Field()
