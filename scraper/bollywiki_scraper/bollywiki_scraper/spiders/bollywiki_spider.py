import scrapy
import pandas as pd
from io import StringIO
from bs4 import BeautifulSoup



class BollywikiSpiderSpider(scrapy.Spider):
    name = "bollywiki_spider"
    allowed_domains = ["en.wikipedia.org"]

    # urls = pd.read_csv('data/urls.csv')
    # start_urls = urls['start_urls'].tolist()
    start_urls = ['https://en.wikipedia.org/wiki/List_of_Hindi_films_of_2024']
    
    def parse(self, response):
        self.logger.warning(f"response: {response.url}")
        
        tables = response.xpath('//table[@class="wikitable"]')
        self.logger.warning(f"# tables: {len(tables)}")

        for table in tables:
            items = self.process_table(table, response)
            for item in items:
                yield item
            break

    def process_table(self, table, response):
        table = self.preprocess_html(table) # fix ul issue
        df = pd.read_html(StringIO(table))[0] # string to dataframe
        self.logger.warning(f"table: {df}")

        # transformations
        # round 3
        column_names = ['opening_month', 'opening_day', 'title', 'director', 'cast', 'studio', 'ref']
        df.columns = column_names

        # round 7
        df['year'] = response.url.split('_')[-1] 
        df['opening_day'] = df['opening_day'].apply(str)
        df['opening_month'] = df['opening_month'].str.replace(' ', '')

        df['date'] = df.loc[:, ['opening_month', 'opening_day', 'year']].apply(lambda x: ','.join(x.tolist()), axis=1)
        df['date'] = pd.to_datetime(df['date'], format='%b,%d,%Y')
        df['date'] = df['date'].dt.strftime('%Y-%m-%d')

        for i, row in df.iterrows():
            item = BollywikiScraperItem()
            # round 4
            item['title'] = row['title']
            # round 5
            item['director'] = row['director']
            # round 6
            item['cast'] = row['cast']
            # round 7
            item['opening_year'] = row['year']
            item['opening_date'] = row['date']

            self.logger.warning(f"item: {item}")
            yield item

    def preprocess_html(self, table):
        table = table.get() # selector to string

        # Parse the HTML with BeautifulSoup
        soup = BeautifulSoup(table, 'html.parser')

        # Find all unordered lists
        unordered_lists = soup.find_all('ul')

        for ul in unordered_lists:
            # Extract text from each list item and join them with a comma
            text = ','.join(li.get_text() for li in ul.find_all('li'))
            # Replace the ul contents with the comma-separated text
            ul.string = text

        modified_html = str(soup)
        return modified_html



class BollywikiScraperItem(scrapy.Item):
    title = scrapy.Field()
    director = scrapy.Field()
    cast = scrapy.Field()

    opening_year = scrapy.Field()
    opening_date = scrapy.Field()


# class BollywikiSpiderSpider(scrapy.Spider):
#     name = "bollywiki_spider"
#     allowed_domains = ["en.wikipedia.org"]

#     # urls = pd.read_csv('data/urls.csv')
#     # start_urls = urls['start_urls'].tolist()
#     start_urls = ['https://en.wikipedia.org/wiki/List_of_Hindi_films_of_2024']
    
#     def parse(self, response):
#         self.logger.info(f"response: {response.url}")
        
#         tables = response.xpath('//table[@class="wikitable"]')
#         self.logger.info(f"tables: {len(tables)}")

#         for table in tables:
#             # table = self.preprocess_table(table.get())
#             table = self.process_table(table)

#             for tbl in pd.read_html(table):
#                 df_table = self.process_table(tbl)
                
#                 for i, row in df_table.iterrows():
#                     item = BollywikiScraperItem()
#                     # item['opening_month'] = row['opening_month']
#                     # item['opening_day'] = row['opening_day']

#                     item['title'] = row['title']
#                     item['director_list'] = [c.strip() for c in row['director'].split(',') if row['director'] != '']
#                     item['cast_list'] = [c.strip() for c in row['cast'].split(',') if row['cast'] != '']
#                     item['studio_list'] = [c.strip() for c in row['studio'].split(',') if row['studio'] != '']

#                     yield item

#     def process_table(self, table):
#         tables = pd.read_html(table.get())
#         assert len(tables) == 1, "Should only be one table"
#         table = tables[0]

#         table.columns = ['opening_month', 'opening_day', 'title', 'director', 'cast', 'studio', 'ref']
#         table = table.fillna('')

#         # self.logger.info(f"table: {table}")
#         return table
    
#     def preprocess_table(self, table_html):
#         """Fixes the HTML in the table by replacing unordered lists with comma-separated text"""
#         soup = BeautifulSoup(table_html, 'html.parser')

#         # Find all unordered lists
#         unordered_lists = soup.find_all('ul')

#         for ul in unordered_lists:
#             # Extract text from each list item and join them with a comma
#             text = ', '.join(li.get_text() for li in ul.find_all('li'))
#             # Replace the ul contents with the comma-separated text
#             ul.string = text

#         # Now the HTML is modified with comma-separated values in place of lists
#         # Convert it back to a string and use pandas.read_html
#         modified_html = str(soup)
#         return modified_html


# import scrapy

# class BollywikiScraperItem(scrapy.Item):
#     title = scrapy.Field()
#     title_link = scrapy.Field()

    # def load_item(self, row):
    #     item = BollywikiScraperItem()

    #     number_of_elements = len(row.xpath('.//td'))

    #     if number_of_elements == 7:
    #         title_cell_link_text = row.xpath('.//td[3]/i/a')
    #         if title_cell_link_text:
    #             item['title'] = title_cell_link_text.xpath('text()').get().strip()
    #             item['title_link'] = title_cell_link_text.xpath('@href').get()
    #     elif number_of_elements == 6:
    #         title_cell_link_text = row.xpath('.//td[2]/i/a')
    #         if title_cell_link_text:
    #             item['title'] = title_cell_link_text.xpath('text()').get().strip()
    #             item['title_link'] = title_cell_link_text.xpath('@href').get()

        # opening_date_colspan = row.xpath('.//th[contains(@style, "width:6%")]/@colspan').get()
        # if opening_date_colspan:
        #     item['opening_year'] = opening_date_colspan.strip()

        # return item

        # loader = ItemLoader(item=item, selector=selector)
        # loader.add_xpath('title', './/td[2]/i/a/text()')
        # self.logger.info(f"selector: {selector.get()}")
        # header = ['opening_month', 'opening_day', 'title', 'director', 'cast', 'studio', 'ref']

        # val = selector.xpath(".//td[3]//a/text()").getall()
        # self.logger.info(f"val: {val}")
        # loader.add_value('title', val)
        # loader.add_xpath('title', './/td[3]//a/text()')
        # loader.add_xpath('title_link', './/td[3]//a/@href')
    
        # return loader.load_item()
    
# row = rows[1]
# row.xpath(".//td").getall()
# row.xpath(".//td/text()").get()

