# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy

class BollywikiScraperItem(scrapy.Item):
    opening_date = scrapy.Field()
    opening_year = scrapy.Field()

    title = scrapy.Field()
    title_url = scrapy.Field()

    director_list = scrapy.Field()

    cast_list = scrapy.Field()

    studio_list = scrapy.Field()

    distrubutor_list = scrapy.Field()

    domestic_gross_num = scrapy.Field()
    worldwide_gross_num = scrapy.Field()

    genre_list = scrapy.Field()
